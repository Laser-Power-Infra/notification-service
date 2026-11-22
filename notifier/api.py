from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal
from urllib.parse import quote
from uuid import uuid4

import aio_pika
import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError

from notifier.config import ApiSettings
from notifier.envelope import TelegramEnvelope, WhatsAppEnvelope
from notifier.topology import EXCHANGE

DEFAULT_TEST_TEXT = "Test message from notifier ✅"

Publish = Callable[[str, bytes], Awaitable[None]]


class NotRouted(Exception):
    """The exchange does not exist or no queue is bound: the worker has never declared the topology."""


def amqp_publisher(amqp_url: str) -> Publish:
    async def publish(routing_key: str, body: bytes) -> None:
        # ponytail: one connection per call; fine for manual test routes, pool it if producers use this.
        connection = await aio_pika.connect(amqp_url, timeout=5)
        async with connection:
            channel = await connection.channel(on_return_raises=True)
            try:
                exchange = await channel.get_exchange(EXCHANGE)  # passive declare
                await exchange.publish(
                    aio_pika.Message(body, content_type="application/json",
                                     delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key=routing_key,
                )
            except (aio_pika.exceptions.ChannelNotFoundEntity, aio_pika.exceptions.PublishError):
                raise NotRouted() from None
    return publish


class TelegramTest(BaseModel):
    chat_id: int
    text: str = DEFAULT_TEST_TEXT
    parse_mode: Literal["HTML", "MarkdownV2"] | None = None


class WhatsAppTest(BaseModel):
    to: str
    text: str = DEFAULT_TEST_TEXT


class TestAccepted(BaseModel):
    id: str
    notification_platform: str


_UPDATE_KINDS = ("message", "edited_message", "channel_post", "edited_channel_post",
                 "my_chat_member", "chat_member")


class TelegramChat(BaseModel):
    chat_id: int
    type: str
    title: str | None = None
    migrated_to: int | None = None


class WhatsAppGroup(BaseModel):
    group_id: str
    name: str | None = None
    size: int | None = None
    announce: bool | None = None


class WhatsAppMessage(BaseModel):
    id: str | None = None
    timestamp: datetime | None = None
    sender: str | None = None
    sender_name: str | None = None
    from_me: bool = False
    type: str | None = None
    text: str | None = None


class WhatsAppMessages(BaseModel):
    group_id: str
    start: datetime | None = None
    end: datetime | None = None
    count: int
    messages: list[WhatsAppMessage]


# Naive dates/datetimes from callers are read as India time; returned timestamps use it too.
IST = timezone(timedelta(hours=5, minutes=30))
_PAGE_SIZE = 100
_MAX_PAGES = 100  # 10,000 messages per request


def _message_text(message: object) -> str | None:
    if not isinstance(message, dict):
        return None
    if isinstance(message.get("conversation"), str):
        return message["conversation"]
    for kind, field in (("extendedTextMessage", "text"), ("imageMessage", "caption"),
                        ("videoMessage", "caption"), ("documentMessage", "caption"),
                        ("documentWithCaptionMessage", None)):
        part = message.get(kind)
        if not isinstance(part, dict):
            continue
        if field is None:  # wrapper around a documentMessage
            return _message_text(part.get("message"))
        if isinstance(part.get(field), str):
            return part[field]
    return None


def whatsapp_message(record: dict) -> WhatsAppMessage:
    key = record.get("key") if isinstance(record.get("key"), dict) else {}
    ts = record.get("messageTimestamp")
    return WhatsAppMessage(
        id=key.get("id"),
        timestamp=datetime.fromtimestamp(int(ts), IST) if isinstance(ts, (int, float, str)) and str(ts).isdigit() else None,
        # participantAlt is the phone-number JID when WhatsApp addresses the sender by a hidden LID.
        sender=key.get("participantAlt") or key.get("participant"),
        sender_name=record.get("pushName"),
        from_me=bool(key.get("fromMe")),
        type=record.get("messageType"),
        text=_message_text(record.get("message")),
    )


def _range_bound(value: str, end: bool) -> datetime:
    """Parse '2026-11-01' (whole day) or an ISO datetime; naive values are India time."""
    try:
        if len(value) == 10:
            parsed = datetime.combine(date.fromisoformat(value), time.max if end else time.min)
        else:
            if "T" in value:  # an unencoded '+05:30' in a URL arrives as ' 05:30'
                value = value.replace(" ", "+")
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(422, f"invalid date {value!r}: use YYYY-MM-DD or YYYY-MM-DDTHH:MM") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=IST)


def telegram_chats(updates: list) -> list[TelegramChat]:
    chats: dict[int, TelegramChat] = {}
    migrations: dict[int, int] = {}
    for update in updates:
        if not isinstance(update, dict):
            continue
        for kind in _UPDATE_KINDS:
            item = update.get(kind)
            chat = item.get("chat") if isinstance(item, dict) else None
            if not isinstance(chat, dict) or not isinstance(chat.get("id"), int):
                continue
            name = (chat.get("title")
                    or " ".join(filter(None, [chat.get("first_name"), chat.get("last_name")]))
                    or chat.get("username"))
            chats[chat["id"]] = TelegramChat(chat_id=chat["id"], type=str(chat.get("type", "")), title=name)
            if isinstance(item.get("migrate_to_chat_id"), int):
                migrations[chat["id"]] = item["migrate_to_chat_id"]
    for chat_id, new_id in migrations.items():
        chats[chat_id].migrated_to = new_id
    return list(chats.values())


def whatsapp_groups(groups: list) -> list[WhatsAppGroup]:
    return [
        WhatsAppGroup(group_id=g["id"], name=g.get("subject"), size=g.get("size"), announce=g.get("announce"))
        for g in groups if isinstance(g, dict) and isinstance(g.get("id"), str)
    ]


async def _get_json(http: httpx.AsyncClient, service: str, url: str, method: str = "GET", **kwargs) -> object:
    # Errors carry only status or exception type: URLs and headers hold secrets.
    try:
        resp = await http.request(method, url, **kwargs)
    except Exception as e:
        raise HTTPException(502, f"{service}: {type(e).__name__}") from None
    if resp.status_code != 200:
        raise HTTPException(502, f"{service}: {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        raise HTTPException(502, f"{service}: invalid JSON") from None


def create_app(settings: ApiSettings | None = None, http: httpx.AsyncClient | None = None,
               publish: Publish | None = None) -> FastAPI:
    settings = settings or ApiSettings()
    if publish is None and settings.amqp_url:
        publish = amqp_publisher(settings.amqp_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.http is not None:  # injected (tests)
            yield
            return
        async with httpx.AsyncClient(timeout=10) as client:
            app.state.http = client
            yield
            app.state.http = None

    app = FastAPI(title="Notifier lookup API", lifespan=lifespan)
    app.state.http = http
    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    if origins:
        app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["GET"], allow_headers=["*"])

    def require(channel: str) -> None:
        if settings.missing_for(channel):
            raise HTTPException(503, f"{channel} not configured")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/telegram/chats", response_model=list[TelegramChat])
    async def get_telegram_chats(request: Request) -> list[TelegramChat]:
        """Chats that messaged the bot in roughly the last 24 hours (Telegram getUpdates)."""
        require("telegram")
        token = settings.telegram_bot_token.get_secret_value()
        body = await _get_json(request.app.state.http, "telegram",
                               f"https://api.telegram.org/bot{token}/getUpdates")
        if not (isinstance(body, dict) and body.get("ok") is True and isinstance(body.get("result"), list)):
            raise HTTPException(502, "telegram: unexpected response")
        return telegram_chats(body["result"])

    @app.get("/whatsapp/groups", response_model=list[WhatsAppGroup])
    async def get_whatsapp_groups(request: Request) -> list[WhatsAppGroup]:
        """Groups the connected WhatsApp account is in; group_id goes in recipient.to."""
        require("whatsapp")
        url = (f"{settings.evolution_url.rstrip('/')}/group/fetchAllGroups/"
               f"{quote(settings.evolution_instance, safe='')}")
        body = await _get_json(request.app.state.http, "whatsapp", url,
                               params={"getParticipants": "false"},
                               headers={"apikey": settings.evolution_api_key.get_secret_value()})
        if not isinstance(body, list):
            raise HTTPException(502, "whatsapp: unexpected response")
        return whatsapp_groups(body)

    @app.get("/whatsapp/groups/{group_id}/messages", response_model=WhatsAppMessages)
    async def get_whatsapp_group_messages(
        request: Request, group_id: str,
        start: str | None = Query(None, description="From, e.g. 2026-11-01 or 2026-11-01T09:00 (India time unless "
                                                    "an offset is given). Omit for no lower limit."),
        end: str | None = Query(None, description="To, inclusive; a bare date covers the whole day. "
                                                  "Omit for no upper limit."),
    ) -> WhatsAppMessages:
        """Messages in a WhatsApp group, oldest first. Omit start and end to get every stored message.

        Only messages Evolution has stored are returned: those received since the account was linked,
        plus whatever history WhatsApp synced at link time."""
        require("whatsapp")
        if not group_id.endswith("@g.us"):
            raise HTTPException(422, "group_id must end with @g.us (see GET /whatsapp/groups)")
        lo = _range_bound(start, end=False) if start else None
        hi = _range_bound(end, end=True) if end else None
        if lo and hi and lo > hi:
            raise HTTPException(422, "start must be before end")
        url = (f"{settings.evolution_url.rstrip('/')}/chat/findMessages/"
               f"{quote(settings.evolution_instance, safe='')}")
        headers = {"apikey": settings.evolution_api_key.get_secret_value()}
        where: dict = {"key": {"remoteJid": group_id}}
        if lo or hi:
            # Evolution applies the timestamp filter only when both bounds are present.
            where["messageTimestamp"] = {
                "gte": (lo or datetime(1970, 1, 1, tzinfo=timezone.utc)).isoformat(),
                "lte": (hi or datetime(2100, 1, 1, tzinfo=timezone.utc)).isoformat(),
            }
        records: list = []
        page, pages = 1, 1
        while page <= pages:
            if page > _MAX_PAGES:
                raise HTTPException(413, f"more than {_MAX_PAGES * _PAGE_SIZE} messages; narrow the date range")
            body = await _get_json(request.app.state.http, "whatsapp", url, method="POST", headers=headers,
                                   json={"where": where, "page": page, "offset": _PAGE_SIZE})
            msgs = body.get("messages") if isinstance(body, dict) else None
            if not (isinstance(msgs, dict) and isinstance(msgs.get("records"), list)):
                raise HTTPException(502, "whatsapp: unexpected response")
            records += [r for r in msgs["records"] if isinstance(r, dict)]
            pages = msgs.get("pages") if isinstance(msgs.get("pages"), int) else 1
            page += 1
        messages = sorted((whatsapp_message(r) for r in records),
                          key=lambda m: m.timestamp or datetime.min.replace(tzinfo=IST))
        return WhatsAppMessages(group_id=group_id, start=lo, end=hi, count=len(messages), messages=messages)

    async def publish_test(model: type[TelegramEnvelope] | type[WhatsAppEnvelope], platform: str,
                           recipient: dict, **fields) -> TestAccepted:
        if settings.amqp_url is None or publish is None:
            raise HTTPException(503, "rabbitmq not configured")
        try:
            env = model(id=uuid4(), version=1, notification_platform=platform, recipient=recipient, **fields)
        except ValidationError as e:
            raise HTTPException(422, [{"loc": err["loc"], "msg": err["msg"]} for err in e.errors()]) from None
        try:
            await publish(platform, env.model_dump_json().encode())
        except NotRouted:
            raise HTTPException(503, "rabbitmq: message not routed (is the notifier worker running?)") from None
        except Exception as e:  # type name only: AMQP errors can contain the connection URL with password
            raise HTTPException(502, f"rabbitmq: {type(e).__name__}") from None
        return TestAccepted(id=str(env.id), notification_platform=platform)

    @app.post("/telegram/test", status_code=202, response_model=TestAccepted)
    async def telegram_test(req: TelegramTest) -> TestAccepted:
        """Publish a test notification through RabbitMQ; follow its id in the worker log."""
        return await publish_test(TelegramEnvelope, "telegram", {"chat_id": req.chat_id},
                                  text=req.text, parse_mode=req.parse_mode)

    @app.post("/whatsapp/test", status_code=202, response_model=TestAccepted)
    async def whatsapp_test(req: WhatsAppTest) -> TestAccepted:
        """Publish a test notification through RabbitMQ; follow its id in the worker log."""
        return await publish_test(WhatsAppEnvelope, "whatsapp", {"to": req.to}, text=req.text)

    return app


app = create_app()
