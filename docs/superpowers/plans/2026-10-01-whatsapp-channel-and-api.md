# WhatsApp Channel and Lookup API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a WhatsApp delivery worker (Evolution API) next to the existing Telegram worker, plus a FastAPI lookup API for Telegram chat IDs and WhatsApp group IDs.

**Architecture:** One codebase and image. The existing worker becomes channel-aware: `python -m notifier.main --channel telegram|whatsapp` picks the queue names and the sender. A new stateless FastAPI app (`notifier/api.py`) proxies Telegram `getUpdates` and Evolution `fetchAllGroups`. Evolution API, its Postgres and Redis run in docker-compose.

**Tech Stack:** Python 3.12, uv, aio-pika, httpx, pydantic v2, pydantic-settings, FastAPI, uvicorn, pytest, pytest-asyncio, RabbitMQ 4, Evolution API v2 (`evoapicloud/evolution-api`).

**Spec:** `docs/superpowers/specs/2026-10-01-whatsapp-channel-and-api-design.md` (builds on `docs/superpowers/specs/2026-10-01-telegram-notification-service-design.md`)

## Global Constraints

- Packages only via `uv` (`uv add`, `uv run`). No `pip`, no `poetry`, no `requirements.txt`.
- **No git commits.** The user's rule. Do not run `git add`/`git commit`.
- Telegram queue names, routing key and envelope format must not change: `notify.telegram`, `telegram.send`, `notify.retry.<s>s`, `notify.dlq`.
- WhatsApp names: routing key `whatsapp.send`, main queue `notify.whatsapp` (quorum, `x-delivery-limit: 20`), retry queues `notify.whatsapp.retry.<s>s`, DLQ `notify.whatsapp.dlq`.
- WhatsApp `recipient.to`: `^\d+@g\.us$` or optional `+` then 8 to 15 digits; `+` stripped before sending.
- WhatsApp `text`: 1 to 65,536 characters, not whitespace-only.
- WhatsApp rate defaults: 2 per second overall, 1 per second per recipient. Telegram: 25 per second overall, 1 per second per chat.
- API: FastAPI, port 8000, no auth, compose binds `127.0.0.1:8000`. Upstream failure returns `502` with `{"detail": "<service>: <status or error type>"}`. Channel not configured returns `503` with `{"detail": "<channel> not configured"}`.
- Tokens and API keys never appear in logs, `x-error` headers, or API responses.

## Review Focus

1. **Evolution answers 400 while the WhatsApp session is disconnected.** If treated as permanent, every message during an outage is dead-lettered. Expect: retryable. Task 2 maps 400 bodies containing "Connection Closed" to retryable and tests it; Task 7 verifies the real reply.
2. **Telegram envelope published to `whatsapp.send` (or the reverse).** Expect: DLQ with a clear channel-mismatch reason, nothing sent. Test in Task 4.
3. **Blank env vars from compose (`EVOLUTION_URL=`) for a channel the container does not use.** Expect: treated as unset, so the container starts; the channel that needs it fails fast with a clear message. Tests in Task 3.
4. **Evolution `fetchAllGroups` returns a non-list (error object) with HTTP 200, or Telegram returns `ok: false`.** Expect: `502`, never a 500 or a crash. Tests in Task 5.
5. **Phone number written with spaces or dashes (`+91 98765-43210`).** Expect: rejected at validation into the DLQ with a readable reason, not sent to a wrong number. Test in Task 1.

---

## File Structure

```
notifier/__init__.py      # MODIFY: silence httpx/httpcore loggers for every entry point
notifier/results.py       # CREATE: Ok(message_id), Retryable, Permanent, SendResult
notifier/envelope.py      # MODIFY: TelegramEnvelope, WhatsAppEnvelope, parse_envelope(), recipient_key()
notifier/telegram.py      # MODIFY: use results.py, return Ok(message_id)
notifier/whatsapp.py      # CREATE: EvolutionClient, classify_evolution()
notifier/config.py        # MODIFY: optional channel creds, missing_for(), ApiSettings, rate settings
notifier/topology.py      # MODIFY: ChannelNames, CHANNELS, declare(..., names), Publisher(..., dlq)
notifier/handler.py       # MODIFY: channel-aware, parse_envelope, recipient_key, message_id in logs
notifier/ratelimit.py     # MODIFY: string recipient keys (type hints only)
notifier/main.py          # MODIFY: --channel, build_sender(), run(settings, channel, ...)
notifier/api.py           # CREATE: FastAPI app
tests/test_envelope.py    # MODIFY: new names + WhatsApp cases
tests/test_telegram.py    # MODIFY: import names
tests/test_whatsapp.py    # CREATE
tests/test_config.py      # MODIFY: env_ignore_empty semantics + new fields
tests/test_handler.py     # MODIFY: channel mismatch, WhatsApp key
tests/test_api.py         # CREATE
tests/test_integration.py # MODIFY: WhatsApp worker cases
docker-compose.yml        # MODIFY: rename notifier, add whatsapp-worker, api, evolution stack
README.md                 # CREATE: setup and usage
```

---

### Task 1: Shared result types and channel envelopes

**Files:**
- Create: `notifier/results.py`
- Modify: `notifier/__init__.py`, `notifier/envelope.py`, `notifier/telegram.py`
- Test: `tests/test_envelope.py`, `tests/test_telegram.py`

**Interfaces:**
- Produces:
  - `notifier.results`: `Ok(message_id: str | None = None)`, `Retryable(reason: str, retry_after: float | None = None)`, `Permanent(reason: str)`, `SendResult`.
  - `notifier.envelope`: `TelegramRecipient(chat_id: int)`, `TelegramEnvelope` (channel `"telegram"`), `WhatsAppRecipient(to: str)`, `WhatsAppEnvelope` (channel `"whatsapp"`), both with `recipient_key() -> str`; `AnyEnvelope = TelegramEnvelope | WhatsAppEnvelope`; `parse_envelope(body: bytes | str) -> AnyEnvelope` raising `pydantic.ValidationError`.
  - `notifier.telegram` keeps exporting `Ok`, `Retryable`, `Permanent`, `SendResult`, `classify`, `TelegramClient`. `classify` now returns `Ok(str(result.message_id))` when present.

- [ ] **Step 1: Update existing tests to the new names and add new cases**

In `tests/test_envelope.py` replace `from notifier.envelope import Envelope` with:
```python
from notifier.envelope import TelegramEnvelope as Envelope, WhatsAppEnvelope, parse_envelope
```
Append to `tests/test_envelope.py`:
```python
def wa_body(**overrides):
    data = {"id": str(uuid.uuid4()), "version": 1, "channel": "whatsapp",
            "recipient": {"to": "120363295648424210@g.us"}, "text": "*hi*"}
    data.update(overrides)
    return json.dumps(data).encode()


@pytest.mark.parametrize("to, expected", [
    ("120363295648424210@g.us", "120363295648424210@g.us"),
    ("919876543210", "919876543210"),
    ("+919876543210", "919876543210"),
])
def test_whatsapp_recipient_ok(to, expected):
    env = parse_envelope(wa_body(recipient={"to": to}))
    assert isinstance(env, WhatsAppEnvelope)
    assert env.recipient.to == expected
    assert env.recipient_key() == expected


@pytest.mark.parametrize("to", [
    "1234567", "1234567890123456", "+91 98765-43210", "abc", "12345@g.com",
    "@g.us", "919876543210@s.whatsapp.net", "",
])
def test_whatsapp_recipient_rejected(to):
    with pytest.raises(ValidationError):
        parse_envelope(wa_body(recipient={"to": to}))


@pytest.mark.parametrize("overrides", [
    {"text": ""}, {"text": "  \n"}, {"text": "x" * 65537}, {"parse_mode": "HTML"},
    {"recipient": {"to": "919876543210", "x": 1}},
])
def test_whatsapp_invalid(overrides):
    with pytest.raises(ValidationError):
        parse_envelope(wa_body(**overrides))


def test_whatsapp_long_text_ok():
    assert len(parse_envelope(wa_body(text="x" * 65536)).text) == 65536


def test_parse_envelope_picks_telegram():
    env = parse_envelope(body())
    assert isinstance(env, Envelope)
    assert env.recipient_key() == "-1001234567890"


@pytest.mark.parametrize("channel", ["email", None])
def test_parse_envelope_unknown_channel(channel):
    with pytest.raises(ValidationError):
        parse_envelope(body(channel=channel))
```

In `tests/test_telegram.py` replace `from notifier.envelope import Envelope, Recipient` with:
```python
from notifier.envelope import TelegramEnvelope as Envelope, TelegramRecipient as Recipient
```
and add one case to the `test_classify` parametrize list:
```python
        (200, {"ok": True, "result": {"message_id": 55}}, Ok("55")),
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_envelope.py tests/test_telegram.py -q`
Expected: collection ERROR, `ImportError: cannot import name 'TelegramEnvelope'`.

- [ ] **Step 3: Create `notifier/results.py`**

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class Ok:
    message_id: str | None = None


@dataclass(frozen=True)
class Retryable:
    reason: str
    retry_after: float | None = None


@dataclass(frozen=True)
class Permanent:
    reason: str


SendResult = Ok | Retryable | Permanent
```

- [ ] **Step 4: Move logger silencing to `notifier/__init__.py`**

Write `notifier/__init__.py`:
```python
import logging

# httpx logs every request URL at INFO; Telegram URLs contain the bot token.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
```

- [ ] **Step 5: Rewrite `notifier/envelope.py`**

```python
import re
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictInt, TypeAdapter, field_validator


def _not_blank(v: str) -> str:
    # Both APIs reject whitespace-only text; fail early with a clear reason.
    if not v.strip():
        raise ValueError("text must not be blank")
    return v


_WA_TO = re.compile(r"\+?\d{8,15}|\d+@g\.us")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TelegramRecipient(_Strict):
    chat_id: StrictInt


class TelegramEnvelope(_Strict):
    id: UUID
    version: Literal[1]
    channel: Literal["telegram"]
    recipient: TelegramRecipient
    text: Annotated[str, Field(min_length=1, max_length=4096), AfterValidator(_not_blank)]
    parse_mode: Literal["HTML", "MarkdownV2"] | None = None
    disable_notification: bool = False

    def recipient_key(self) -> str:
        return str(self.recipient.chat_id)


class WhatsAppRecipient(_Strict):
    to: str

    @field_validator("to")
    @classmethod
    def _check_to(cls, v: str) -> str:
        if not _WA_TO.fullmatch(v):
            raise ValueError("to must be a group id like 1203...@g.us or a phone number with country code, digits only")
        return v.removeprefix("+")


class WhatsAppEnvelope(_Strict):
    id: UUID
    version: Literal[1]
    channel: Literal["whatsapp"]
    recipient: WhatsAppRecipient
    text: Annotated[str, Field(min_length=1, max_length=65536), AfterValidator(_not_blank)]

    def recipient_key(self) -> str:
        return self.recipient.to


AnyEnvelope = TelegramEnvelope | WhatsAppEnvelope
_adapter = TypeAdapter(Annotated[AnyEnvelope, Field(discriminator="channel")])


def parse_envelope(body: bytes | str) -> AnyEnvelope:
    return _adapter.validate_json(body)
```

- [ ] **Step 6: Update `notifier/telegram.py`**

Delete the `logging` import, the two `logging.getLogger(...)` lines, and the `Ok`/`Retryable`/`Permanent`/`SendResult` definitions. Replace the imports at the top with:
```python
import httpx

from notifier.envelope import TelegramEnvelope
from notifier.results import Ok, Permanent, Retryable, SendResult
```
In `classify`, replace `return Ok()` with:
```python
        result = body.get("result")
        message_id = result.get("message_id") if isinstance(result, dict) else None
        return Ok(str(message_id) if message_id is not None else None)
```
Change the `send` signature to `async def send(self, env: TelegramEnvelope) -> SendResult:`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_envelope.py tests/test_telegram.py tests/test_handler.py -q`
Expected: all PASS. (`test_handler.py` still passes because `handler.py` imports `Envelope` — if it fails with `ImportError: Envelope`, that is fixed in Task 4; in that case run only the first two files here.)

---

### Task 2: Evolution API sender

**Files:**
- Create: `notifier/whatsapp.py`
- Test: `tests/test_whatsapp.py`

**Interfaces:**
- Consumes: `WhatsAppEnvelope` (Task 1), `Ok`/`Retryable`/`Permanent`/`SendResult` (Task 1).
- Produces: `classify_evolution(status: int, body: object) -> SendResult`; `EvolutionClient(base_url: str, api_key: str, instance: str, http: httpx.AsyncClient)` with `async send(env: WhatsAppEnvelope) -> SendResult`, never raising.

- [ ] **Step 1: Write the failing test**

`tests/test_whatsapp.py`:
```python
import json
import uuid

import httpx
import pytest

from notifier.envelope import WhatsAppEnvelope, WhatsAppRecipient
from notifier.results import Ok, Permanent, Retryable
from notifier.whatsapp import EvolutionClient, classify_evolution

KEY = "EVO-SECRET-KEY"


def env(to="+919876543210"):
    return WhatsAppEnvelope(id=uuid.uuid4(), version=1, channel="whatsapp",
                            recipient=WhatsAppRecipient(to=to), text="*hi*")


def client(handler, base="http://evo:8080/"):
    return EvolutionClient(base, KEY, "my inst", httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_sends_expected_request():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(201, json={"key": {"id": "BAE594145F4C59B4"}, "status": "PENDING"})

    assert await client(handler).send(env()) == Ok("BAE594145F4C59B4")
    req = seen[0]
    assert req.method == "POST"
    assert str(req.url) == "http://evo:8080/message/sendText/my%20inst"
    assert req.headers["apikey"] == KEY
    assert json.loads(req.content) == {"number": "919876543210", "text": "*hi*"}


@pytest.mark.parametrize("status, body, expected", [
    (201, {"key": {"id": "X1"}}, Ok("X1")),
    (200, {"key": {"id": "X2"}}, Ok("X2")),
    (201, {}, Ok(None)),
    (400, {"status": 400, "error": "Bad Request",
           "response": {"message": [{"exists": False, "jid": "91@s.whatsapp.net", "number": "91"}]}},
     Permanent('400 [{"exists": false, "jid": "91@s.whatsapp.net", "number": "91"}]')),
    (400, {"response": {"message": ["Connection Closed"]}}, Retryable('400 ["Connection Closed"]')),
    (401, {"error": "Unauthorized"}, Retryable("401 Unauthorized")),
    (404, {"response": {"message": ["The \"x\" instance does not exist"]}},
     Retryable('404 ["The \\"x\\" instance does not exist"]')),
    (500, None, Retryable("500")),
])
def test_classify_evolution(status, body, expected):
    assert classify_evolution(status, body) == expected


async def test_non_json_body_is_retryable():
    assert await client(lambda r: httpx.Response(502, text="<html>")).send(env()) == Retryable("502")


async def test_network_error_is_retryable_without_key():
    def handler(request):
        raise httpx.ConnectError("boom")

    result = await client(handler).send(env())
    assert result == Retryable("network error: ConnectError")
    assert KEY not in result.reason
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_whatsapp.py -q`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'notifier.whatsapp'`.

- [ ] **Step 3: Implement `notifier/whatsapp.py`**

```python
import json
from urllib.parse import quote

import httpx

from notifier.envelope import WhatsAppEnvelope
from notifier.results import Ok, Permanent, Retryable, SendResult


def _detail(body: dict) -> str:
    resp = body.get("response")
    msg = resp.get("message") if isinstance(resp, dict) else None
    if msg is None:
        msg = body.get("error") or body.get("message") or ""
    return (msg if isinstance(msg, str) else json.dumps(msg, default=str))[:200]


def classify_evolution(status: int, body: object) -> SendResult:
    body = body if isinstance(body, dict) else {}
    if status in (200, 201):
        key = body.get("key")
        message_id = key.get("id") if isinstance(key, dict) else None
        return Ok(str(message_id) if message_id else None)
    detail = _detail(body)
    reason = f"{status} {detail}".strip()
    # A disconnected WhatsApp session is reported as 400 "Connection Closed": retry, do not dead-letter.
    if status == 400 and "connection closed" not in detail.lower():
        return Permanent(reason)
    # 401/403 bad key, 404 unknown instance, 5xx: retry so a config fix or recovery still delivers.
    return Retryable(reason)


class EvolutionClient:
    def __init__(self, base_url: str, api_key: str, instance: str, http: httpx.AsyncClient):
        self._url = f"{base_url.rstrip('/')}/message/sendText/{quote(instance, safe='')}"
        self._headers = {"apikey": api_key}
        self._http = http

    async def send(self, env: WhatsAppEnvelope) -> SendResult:
        try:
            resp = await self._http.post(self._url, json={"number": env.recipient.to, "text": env.text},
                                         headers=self._headers)
        except Exception as e:  # never surface exception text: keep logs free of secrets
            return Retryable(f"network error: {type(e).__name__}")
        try:
            body = resp.json()
        except ValueError:
            body = None
        return classify_evolution(resp.status_code, body)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_whatsapp.py -q`
Expected: all PASS.

---

### Task 3: Settings for workers and API

**Files:**
- Modify: `notifier/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces:
  - `ApiSettings` (all optional): `telegram_bot_token: SecretStr | None`, `evolution_url: str | None`, `evolution_api_key: SecretStr | None`, `evolution_instance: str | None`, `log_level: str = "INFO"`, `missing_for(channel: str) -> list[str]` returning env var names.
  - `Settings(ApiSettings)` adds `amqp_url: str` (required), `prefetch: int = 10`, `retry_delays: list[int] = [10, 60, 300]`, `rate_global_per_sec: float | None = None`, `rate_per_recipient_interval: float = 1.0`.
  - Empty env vars are treated as unset (`env_ignore_empty=True`).

- [ ] **Step 1: Update and extend tests**

In `tests/test_config.py`:
- Change the bad-values parametrize to `@pytest.mark.parametrize("bad", ["0", "-5", "10,10", "x"])` (empty now means "use the default").
- Replace `test_empty_token_rejected` with:
```python
def test_empty_values_count_as_unset(monkeypatch):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("EVOLUTION_URL", "")
    monkeypatch.setenv("RETRY_DELAYS", "")
    s = Settings()
    assert s.telegram_bot_token is None and s.evolution_url is None
    assert s.retry_delays == [10, 60, 300]
    assert s.missing_for("telegram") == ["TELEGRAM_BOT_TOKEN"]
```
Append:
```python
from notifier.config import ApiSettings


def test_missing_for_whatsapp(monkeypatch):
    for k in ("TELEGRAM_BOT_TOKEN", "EVOLUTION_URL", "EVOLUTION_API_KEY", "EVOLUTION_INSTANCE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("EVOLUTION_URL", "http://evo:8080")
    s = Settings()
    assert s.missing_for("whatsapp") == ["EVOLUTION_API_KEY", "EVOLUTION_INSTANCE"]
    assert s.missing_for("telegram") == ["TELEGRAM_BOT_TOKEN"]


def test_rate_defaults(monkeypatch):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    s = Settings()
    assert s.rate_global_per_sec is None and s.rate_per_recipient_interval == 1.0


def test_api_settings_need_no_amqp(monkeypatch):
    monkeypatch.delenv("AMQP_URL", raising=False)
    monkeypatch.setenv("EVOLUTION_API_KEY", "k")
    s = ApiSettings()
    assert s.evolution_api_key.get_secret_value() == "k"
    assert "k" not in repr(s)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_config.py -q`
Expected: collection ERROR, `ImportError: cannot import name 'ApiSettings'`.

- [ ] **Step 3: Rewrite `notifier/config.py`**

```python
from typing import Annotated, Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

_REQUIRED = {
    "telegram": ("telegram_bot_token",),
    "whatsapp": ("evolution_url", "evolution_api_key", "evolution_instance"),
}


class ApiSettings(BaseSettings):
    # hide_input_in_errors: validation errors would otherwise echo env values, including secrets.
    # env_ignore_empty: compose passes blank values for channels a container does not use.
    model_config = SettingsConfigDict(hide_input_in_errors=True, env_ignore_empty=True)

    telegram_bot_token: SecretStr | None = None
    evolution_url: str | None = None
    evolution_api_key: SecretStr | None = None
    evolution_instance: str | None = None
    log_level: str = "INFO"

    def missing_for(self, channel: str) -> list[str]:
        return [name.upper() for name in _REQUIRED[channel] if getattr(self, name) is None]


class Settings(ApiSettings):
    amqp_url: str
    prefetch: int = Field(10, ge=1)
    retry_delays: Annotated[list[int], NoDecode] = [10, 60, 300]
    rate_global_per_sec: float | None = Field(None, gt=0)  # None: per-channel default
    rate_per_recipient_interval: float = Field(1.0, gt=0)

    @field_validator("retry_delays", mode="before")
    @classmethod
    def _split(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [int(x) for x in v.split(",") if x.strip()]
        return v

    @field_validator("retry_delays")
    @classmethod
    def _check(cls, v: list[int]) -> list[int]:
        # Delays name the retry queues, so they must be positive and unique.
        if not v or any(d <= 0 for d in v) or len(set(v)) != len(v):
            raise ValueError("RETRY_DELAYS must be unique positive integers, e.g. 10,60,300")
        return v
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -q`
Expected: all PASS.

---

### Task 4: Channel-aware worker (topology, handler, entry point)

**Files:**
- Modify: `notifier/topology.py`, `notifier/handler.py`, `notifier/ratelimit.py`, `notifier/main.py`
- Test: `tests/test_handler.py`, `tests/test_main.py` (create), `tests/test_integration.py`

**Interfaces:**
- Consumes: `parse_envelope`, `AnyEnvelope` (Task 1); `EvolutionClient` (Task 2); `Settings.missing_for`, `rate_*` (Task 3).
- Produces:
  - `topology`: `ChannelNames(routing_key, main, dlq, retry_prefix)` with `retry(delay) -> str`; `CHANNELS: dict[str, ChannelNames]`; existing constants `EXCHANGE`, `ROUTING_KEY`, `MAIN_QUEUE`, `DLQ_QUEUE` keep their Telegram values; `declare(channel, retry_delays, names=CHANNELS["telegram"])`; `Publisher(channel, dlq=DLQ_QUEUE)`.
  - `handler.Handler(sender, limiter, publisher, retry_queues, *, channel="telegram", ...)`.
  - `main.run(settings, channel="telegram", http=None, stop=None)`; raises `ValueError` naming missing env vars before connecting. `main.main(argv=None)` with `--channel`.

- [ ] **Step 1: Write failing tests**

Append to `tests/test_handler.py`:
```python
def wa_body(to="919876543210"):
    return json.dumps({"id": str(uuid.uuid4()), "version": 1, "channel": "whatsapp",
                       "recipient": {"to": to}, "text": "hi"}).encode()


async def test_channel_mismatch_goes_to_dlq():
    h, tg, pub = make()
    await h.handle(wa_body(), {})
    assert tg.calls == 0
    assert pub.dead == ["channel mismatch: got whatsapp, this worker serves telegram"]


async def test_whatsapp_worker_sends_and_limits_by_recipient():
    keys = []

    class RecordingLimiter:
        async def acquire(self, key):
            keys.append(key)

    tg, pub = FakeTelegram(Ok("WA1")), FakePublisher()
    h = Handler(tg, RecordingLimiter(), pub, QUEUES, channel="whatsapp", sleep=no_sleep)
    await h.handle(wa_body("+919876543210"), {})
    assert tg.calls == 1 and keys == ["919876543210"] and pub.dead == []
```
Create `tests/test_main.py`:
```python
import pytest

from notifier.config import Settings
from notifier.main import run


async def test_run_refuses_missing_channel_settings():
    s = Settings(amqp_url="amqp://nowhere:1/", telegram_bot_token=None,
                 evolution_url=None, evolution_api_key=None, evolution_instance=None)
    with pytest.raises(ValueError, match="EVOLUTION_URL, EVOLUTION_API_KEY, EVOLUTION_INSTANCE"):
        await run(s, "whatsapp")
```
Append to `tests/test_integration.py`:
```python
from notifier.topology import CHANNELS

WA = CHANNELS["whatsapp"]


@pytest.fixture
async def wa_channel():
    try:
        conn = await aio_pika.connect(AMQP_URL, timeout=3)
    except Exception:
        pytest.skip("RabbitMQ not reachable; run: docker compose up -d rabbitmq")
    async with conn:
        ch = await conn.channel()
        _, retry_names = await declare(ch, DELAYS, WA)
        for name in [WA.main, WA.dlq, *retry_names]:
            await (await ch.get_queue(name)).purge()
        yield ch


@pytest.fixture
async def wa_service():
    calls = []
    responses = [httpx.Response(201, json={"key": {"id": "WAID1"}})]

    def respond(request):
        calls.append(json.loads(request.content))
        return responses[min(len(calls), len(responses)) - 1]

    http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    settings = Settings(amqp_url=AMQP_URL, evolution_url="http://evo:8080", evolution_api_key="k",
                        evolution_instance="test", retry_delays=DELAYS)
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, "whatsapp", http=http, stop=stop))

    def set_responses(*rs):
        responses[:] = rs

    yield calls, set_responses
    stop.set()
    await asyncio.wait_for(task, 10)
    await http.aclose()


async def publish_wa(ch, to="120363295648424210@g.us"):
    ex = await ch.get_exchange(EXCHANGE)
    body = json.dumps({"id": str(uuid.uuid4()), "version": 1, "channel": "whatsapp",
                       "recipient": {"to": to}, "text": "hello"}).encode()
    await ex.publish(aio_pika.Message(body), routing_key=WA.routing_key)


async def test_whatsapp_delivered(wa_channel, wa_service):
    calls, _ = wa_service
    await publish_wa(wa_channel)
    await wait_for(lambda: len(calls) == 1)
    assert calls[0] == {"number": "120363295648424210@g.us", "text": "hello"}


async def test_whatsapp_400_goes_to_whatsapp_dlq(wa_channel, wa_service):
    calls, set_responses = wa_service
    set_responses(httpx.Response(400, json={"response": {"message": [{"exists": False}]}}))
    await publish_wa(wa_channel, "919876543210")
    q = await wa_channel.get_queue(WA.dlq)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 15
    msg = None
    while msg is None and loop.time() < deadline:
        msg = await q.get(no_ack=True, fail=False)
        if msg is None:
            await asyncio.sleep(0.2)
    assert msg is not None, "no message in notify.whatsapp.dlq"
    assert "400" in str(msg.headers["x-error"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_handler.py tests/test_main.py tests/test_integration.py -q`
Expected: failures/errors: `ImportError` on `CHANNELS`, `TypeError` on `channel=` keyword, and `test_run_refuses_missing_channel_settings` failing.

- [ ] **Step 3: Update `notifier/topology.py`**

Replace everything above `class Publisher` with:
```python
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue

EXCHANGE = "notifications"


@dataclass(frozen=True)
class ChannelNames:
    routing_key: str
    main: str
    dlq: str
    retry_prefix: str

    def retry(self, delay: int) -> str:
        return f"{self.retry_prefix}.{delay}s"


# Telegram names predate the WhatsApp channel and are kept as-is to avoid a migration.
CHANNELS = {
    "telegram": ChannelNames("telegram.send", "notify.telegram", "notify.dlq", "notify.retry"),
    "whatsapp": ChannelNames("whatsapp.send", "notify.whatsapp", "notify.whatsapp.dlq", "notify.whatsapp.retry"),
}
ROUTING_KEY = CHANNELS["telegram"].routing_key
MAIN_QUEUE = CHANNELS["telegram"].main
DLQ_QUEUE = CHANNELS["telegram"].dlq

# Headers the broker adds on dead-lettering/redelivery; do not copy them into new messages.
_BROKER_HEADERS = {"x-death", "x-delivery-count", "x-first-death-exchange",
                   "x-first-death-queue", "x-first-death-reason",
                   "x-last-death-exchange", "x-last-death-queue", "x-last-death-reason"}


async def declare(channel: AbstractChannel, retry_delays: list[int],
                  names: ChannelNames = CHANNELS["telegram"]) -> tuple[AbstractQueue, list[str]]:
    """Idempotent. Changing arguments of an existing queue fails with PRECONDITION_FAILED: delete the queue first."""
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    await channel.declare_queue(names.dlq, durable=True)
    main = await channel.declare_queue(names.main, durable=True, arguments={
        "x-queue-type": "quorum",
        # Poison messages (handler keeps crashing) land in the DLQ instead of looping.
        "x-delivery-limit": 20,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": names.dlq,
    })
    await main.bind(exchange, names.routing_key)

    retry_names = []
    for delay in retry_delays:
        name = names.retry(delay)
        await channel.declare_queue(name, durable=True, arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": names.routing_key,
        })
        retry_names.append(name)
    return main, retry_names
```
In `class Publisher`, change `__init__` and `dead_letter`:
```python
    def __init__(self, channel: AbstractChannel, dlq: str = DLQ_QUEUE):
        self._channel = channel
        self._dlq = dlq
```
```python
    async def dead_letter(self, body: bytes, headers: Mapping[str, Any], reason: str) -> None:
        await self._publish(body, {**_clean(headers), "x-error": reason[:500]}, self._dlq)
```
Delete the old `retry_queue_name` function (no callers outside topology).

- [ ] **Step 4: Update `notifier/handler.py`**

- Replace imports `from notifier.envelope import Envelope` and `from notifier.telegram import ...` with:
```python
from notifier.envelope import AnyEnvelope, parse_envelope
from notifier.results import Ok, Permanent, Retryable, SendResult
```
- `Handler.__init__` signature becomes:
```python
    def __init__(self, sender, limiter, publisher, retry_queues: list[str], *, channel: str = "telegram",
                 dedup_size: int = 10_000, max_inline_wait: float = 60.0, sleep=asyncio.sleep):
        self._sender = sender
        self._channel = channel
```
(rename `self._telegram` to `self._sender` everywhere; keep the other assignments.)
- Replace the body of `handle` from `try: env = Envelope.model_validate_json(body)` down to `self._inflight.add(env.id)` with:
```python
        try:
            env = parse_envelope(body)
        except ValidationError as e:
            reason = f"invalid envelope: {_summarize(e)}"
            await self._publisher.dead_letter(body, headers, reason)
            _log("dead_letter", None, None, attempt, reason)
            return

        key = env.recipient_key()
        if env.channel != self._channel:
            reason = f"channel mismatch: got {env.channel}, this worker serves {self._channel}"
            await self._publisher.dead_letter(body, headers, reason)
            _log("dead_letter", env.id, key, attempt, reason)
            return

        if env.id in self._sent or env.id in self._inflight:
            _log("duplicate", env.id, key, attempt)
            return

        self._inflight.add(env.id)
```
- In the `try` block that follows, compute the result first so the message id can be logged:
```python
        try:
            result = await self._send(env)
            action = decide(result, attempt, self._retry_queues)
            if isinstance(action, Ack):
                self._sent.add(env.id)
                _log("sent", env.id, key, attempt, message_id=result.message_id)
            elif isinstance(action, Retry):
                await self._publisher.retry(body, headers, action.queue, action.attempt)
                _log("retry", env.id, key, attempt, action.reason)
            else:
                await self._publisher.dead_letter(body, headers, action.reason)
                _log("dead_letter", env.id, key, attempt, action.reason)
        finally:
            self._inflight.discard(env.id)
```
- `_send(self, env: AnyEnvelope)`: use `self._sender.send(env)` and `self._limiter.acquire(env.recipient_key())` in both places.
- Replace `_log` with:
```python
def _log(outcome: str, id: UUID | None, recipient: str | None, attempt: int,
         error: str | None = None, *, message_id: str | None = None) -> None:
    fields = {"outcome": outcome, "id": id, "recipient": recipient, "attempt": attempt}
    if error:
        fields["error"] = error
    if message_id:
        fields["message_id"] = message_id
    log.info("notification", extra={"fields": fields})
```

- [ ] **Step 5: Update `notifier/ratelimit.py` type hints**

`self._next_chat: dict[str, float] = {}` and `async def acquire(self, key: str) -> None:`, renaming `chat_id` to `key` inside `acquire`. Update the class docstring to "Reserve a per-recipient slot first, then a global slot, so a busy recipient does not block others."

- [ ] **Step 6: Update `notifier/main.py`**

Add imports:
```python
import argparse

from notifier.topology import CHANNELS, Publisher, declare
from notifier.whatsapp import EvolutionClient
```
(replace the old `from notifier.topology import Publisher, declare`). Add above `run`:
```python
DEFAULT_RATE = {"telegram": 25.0, "whatsapp": 2.0}


def build_sender(settings: Settings, channel: str, http: httpx.AsyncClient):
    if channel == "telegram":
        return TelegramClient(settings.telegram_bot_token.get_secret_value(), http)
    return EvolutionClient(settings.evolution_url, settings.evolution_api_key.get_secret_value(),
                           settings.evolution_instance, http)
```
Change `run`:
```python
async def run(settings: Settings, channel: str = "telegram", http: httpx.AsyncClient | None = None,
              stop: asyncio.Event | None = None) -> None:
    missing = settings.missing_for(channel)
    if missing:
        raise ValueError(f"{channel} worker needs: {', '.join(missing)}")
    names = CHANNELS[channel]
```
(keep the signal-handler block), then inside the exit stack:
```python
        queue, retry_queues = await declare(channel_, settings.retry_delays, names)

        handler = Handler(
            build_sender(settings, channel, http),
            RateLimiter(settings.rate_global_per_sec or DEFAULT_RATE[channel], settings.rate_per_recipient_interval),
            Publisher(channel_, names.dlq),
            retry_queues,
            channel=channel,
        )
```
where the AMQP channel variable is renamed from `channel` to `channel_` (`channel_ = await connection.channel(on_return_raises=True)` and `await channel_.set_qos(...)`) so it does not shadow the channel name. Add `"channel": channel` to the `started` log fields.
Replace `main`:
```python
def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="RabbitMQ notification worker")
    parser.add_argument("--channel", choices=sorted(CHANNELS), default="telegram")
    args = parser.parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run(settings, args.channel))
```

- [ ] **Step 7: Run tests to verify they pass**

RabbitMQ must be running (`docker compose up -d rabbitmq`).
Run: `uv run pytest -q`
Expected: all PASS, including the existing Telegram integration tests and the 2 new WhatsApp ones.

---

### Task 5: FastAPI lookup API

**Files:**
- Create: `notifier/api.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `ApiSettings`, `missing_for` (Task 3).
- Produces: `telegram_chats(updates: list) -> list[TelegramChat]`; `whatsapp_groups(groups: list) -> list[WhatsAppGroup]`; `create_app(settings: ApiSettings | None = None, http: httpx.AsyncClient | None = None) -> FastAPI`; module-level `app = create_app()`.

- [ ] **Step 1: Add dependencies**

Run: `uv add fastapi uvicorn`
Expected: both in `pyproject.toml` dependencies and `uv.lock`.

- [ ] **Step 2: Write the failing test**

`tests/test_api.py`:
```python
import httpx
import pytest

from notifier.api import TelegramChat, create_app, telegram_chats
from notifier.config import ApiSettings

TOKEN = "123:TG-SECRET"
KEY = "EVO-SECRET"

UPDATES = [
    {"update_id": 1, "message": {"chat": {"id": -5420349998, "type": "group", "title": "Ops"},
                                 "migrate_to_chat_id": -1004317452395}},
    {"update_id": 2, "message": {"chat": {"id": -1004317452395, "type": "supergroup", "title": "Ops"},
                                 "migrate_from_chat_id": -5420349998}},
    {"update_id": 3, "message": {"chat": {"id": 42, "type": "private", "first_name": "Bidyut", "last_name": "Das"}}},
    {"update_id": 4, "edited_message": {"chat": {"id": 42, "type": "private", "first_name": "Bidyut"}}},
    {"update_id": 5, "my_chat_member": {"chat": {"id": 7, "type": "private", "username": "solo"}}},
    "junk",
]


def settings(**kw):
    base = dict(telegram_bot_token=TOKEN, evolution_url="http://evo:8080", evolution_api_key=KEY,
                evolution_instance="inst")
    base.update(kw)
    return ApiSettings(**base)


def api(handler, **kw):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(settings(**kw), http=http)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def test_telegram_chats_dedup_and_migration():
    chats = {c.chat_id: c for c in telegram_chats(UPDATES)}
    assert set(chats) == {-5420349998, -1004317452395, 42, 7}
    assert chats[-5420349998].migrated_to == -1004317452395
    assert chats[-1004317452395].migrated_to is None
    assert chats[42].title == "Bidyut"  # latest update wins
    assert chats[7].title == "solo"


async def test_health():
    async with api(lambda r: httpx.Response(500)) as c:
        assert (await c.get("/health")).json() == {"status": "ok"}


async def test_get_telegram_chats():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True, "result": UPDATES})

    async with api(handler) as c:
        r = await c.get("/telegram/chats")
    assert r.status_code == 200
    assert {"chat_id": -5420349998, "type": "group", "title": "Ops", "migrated_to": -1004317452395} in r.json()
    assert seen[0].url.path == f"/bot{TOKEN}/getUpdates"
    assert "offset" not in seen[0].url.params


async def test_get_whatsapp_groups():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=[
            {"id": "120363295648424210@g.us", "subject": "Ops alerts", "size": 12, "announce": False},
            {"subject": "no id, skipped"},
        ])

    async with api(handler) as c:
        r = await c.get("/whatsapp/groups")
    assert r.status_code == 200
    assert r.json() == [{"group_id": "120363295648424210@g.us", "name": "Ops alerts", "size": 12, "announce": False}]
    assert str(seen[0].url) == "http://evo:8080/group/fetchAllGroups/inst?getParticipants=false"
    assert seen[0].headers["apikey"] == KEY


@pytest.mark.parametrize("path, response, detail", [
    ("/telegram/chats", httpx.Response(401, json={"ok": False}), "telegram: 401"),
    ("/telegram/chats", httpx.Response(200, json={"ok": False}), "telegram: unexpected response"),
    ("/telegram/chats", httpx.Response(200, text="<html>"), "telegram: invalid JSON"),
    ("/whatsapp/groups", httpx.Response(200, json={"status": 404, "error": "Not Found"}), "whatsapp: unexpected response"),
    ("/whatsapp/groups", httpx.Response(500), "whatsapp: 500"),
])
async def test_upstream_failures_return_502(path, response, detail):
    async with api(lambda r: response) as c:
        r = await c.get(path)
    assert r.status_code == 502 and r.json() == {"detail": detail}


async def test_network_error_returns_502_without_secrets():
    def handler(request):
        raise httpx.ConnectError(f"cannot reach {request.url}")

    async with api(handler) as c:
        r = await c.get("/telegram/chats")
    assert r.status_code == 502 and r.json() == {"detail": "telegram: ConnectError"}
    assert TOKEN not in r.text


@pytest.mark.parametrize("path, kw, detail", [
    ("/telegram/chats", {"telegram_bot_token": None}, "telegram not configured"),
    ("/whatsapp/groups", {"evolution_instance": None}, "whatsapp not configured"),
])
async def test_not_configured_returns_503(path, kw, detail):
    async with api(lambda r: httpx.Response(200, json=[]), **kw) as c:
        r = await c.get(path)
    assert r.status_code == 503 and r.json() == {"detail": detail}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_api.py -q`
Expected: collection ERROR, `ModuleNotFoundError: No module named 'notifier.api'`.

- [ ] **Step 4: Implement `notifier/api.py`**

```python
from contextlib import asynccontextmanager
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from notifier.config import ApiSettings

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


async def _get_json(http: httpx.AsyncClient, service: str, url: str, **kwargs) -> object:
    # Errors carry only status or exception type: URLs and headers hold secrets.
    try:
        resp = await http.get(url, **kwargs)
    except Exception as e:
        raise HTTPException(502, f"{service}: {type(e).__name__}") from None
    if resp.status_code != 200:
        raise HTTPException(502, f"{service}: {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        raise HTTPException(502, f"{service}: invalid JSON") from None


def create_app(settings: ApiSettings | None = None, http: httpx.AsyncClient | None = None) -> FastAPI:
    settings = settings or ApiSettings()

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

    return app


app = create_app()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_api.py -q`
Expected: all PASS.

- [ ] **Step 6: Full suite**

Run: `uv run pytest -q`
Expected: all PASS.

---

### Task 6: Compose stack, env and README

**Files:**
- Modify: `docker-compose.yml`, `.env` (append only, never print it)
- Create: `README.md`

**Interfaces:**
- Consumes: `python -m notifier.main --channel ...` (Task 4), `notifier.api:app` (Task 5).

- [ ] **Step 1: Rewrite `docker-compose.yml`**

```yaml
x-worker-env: &worker-env
  AMQP_URL: amqp://notifier:notifier@rabbitmq:5672/
  LOG_LEVEL: INFO

services:
  rabbitmq:
    image: rabbitmq:4-management
    environment:
      RABBITMQ_DEFAULT_USER: notifier
      RABBITMQ_DEFAULT_PASS: notifier
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 12

  telegram-worker:
    build: .
    command: ["uv", "run", "--no-sync", "python", "-m", "notifier.main", "--channel", "telegram"]
    environment:
      <<: *worker-env
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
    depends_on:
      rabbitmq:
        condition: service_healthy
    restart: unless-stopped

  whatsapp-worker:
    build: .
    command: ["uv", "run", "--no-sync", "python", "-m", "notifier.main", "--channel", "whatsapp"]
    environment:
      <<: *worker-env
      EVOLUTION_URL: http://evolution-api:8080
      EVOLUTION_API_KEY: ${EVOLUTION_API_KEY:-}
      EVOLUTION_INSTANCE: ${EVOLUTION_INSTANCE:-}
    depends_on:
      rabbitmq:
        condition: service_healthy
      evolution-api:
        condition: service_started
    restart: unless-stopped

  api:
    build: .
    command: ["uv", "run", "--no-sync", "uvicorn", "notifier.api:app", "--host", "0.0.0.0", "--port", "8000"]
    environment:
      LOG_LEVEL: INFO
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
      EVOLUTION_URL: http://evolution-api:8080
      EVOLUTION_API_KEY: ${EVOLUTION_API_KEY:-}
      EVOLUTION_INSTANCE: ${EVOLUTION_INSTANCE:-}
    ports:
      - "127.0.0.1:8000:8000"
    restart: unless-stopped

  evolution-api:
    image: evoapicloud/evolution-api:latest
    environment:
      SERVER_URL: http://localhost:8080
      AUTHENTICATION_API_KEY: ${EVOLUTION_API_KEY:?set EVOLUTION_API_KEY in .env}
      DATABASE_PROVIDER: postgresql
      DATABASE_CONNECTION_URI: postgresql://evolution:evolution@evolution-postgres:5432/evolution?schema=evolution_api
      DATABASE_CONNECTION_CLIENT_NAME: evolution_exchange
      CACHE_REDIS_ENABLED: "true"
      CACHE_REDIS_URI: redis://evolution-redis:6379/6
      CACHE_REDIS_PREFIX_KEY: evolution
      CACHE_LOCAL_ENABLED: "false"
    ports:
      - "127.0.0.1:8080:8080"
    volumes:
      - evolution_instances:/evolution/instances
    depends_on:
      evolution-postgres:
        condition: service_healthy
      evolution-redis:
        condition: service_started
    restart: unless-stopped

  evolution-postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: evolution
      POSTGRES_PASSWORD: evolution
      POSTGRES_DB: evolution
    volumes:
      - evolution_pg:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U evolution"]
      interval: 5s
      timeout: 5s
      retries: 12
    restart: unless-stopped

  evolution-redis:
    image: redis:7
    volumes:
      - evolution_redis:/data
    restart: unless-stopped

volumes:
  evolution_instances:
  evolution_pg:
  evolution_redis:
```
`${EVOLUTION_API_KEY:?}` is safe here because Step 2 always writes it to `.env`, and compose reads `.env` for interpolation.

- [ ] **Step 2: Add Evolution settings to `.env` without printing it**

```bash
grep -q '^EVOLUTION_API_KEY=' .env || echo "EVOLUTION_API_KEY=$(uv run python -c 'import secrets;print(secrets.token_hex(24))')" >> .env
grep -q '^EVOLUTION_INSTANCE=' .env || echo "EVOLUTION_INSTANCE=notifier" >> .env
grep -c '^EVOLUTION_' .env
```
Expected: `2`.

- [ ] **Step 3: Write `README.md`**

```markdown
# Notification service

RabbitMQ consumers that deliver notifications to Telegram and WhatsApp (via Evolution API), plus an internal lookup API for recipient IDs.

## Run

    docker compose up -d --build --remove-orphans

| Service | URL |
|---|---|
| RabbitMQ management | http://localhost:15672 (notifier / notifier) |
| Lookup API docs | http://127.0.0.1:8000/docs |
| Evolution manager | http://127.0.0.1:8080/manager |

Secrets live in `.env` (git-ignored): `TELEGRAM_BOT_TOKEN`, `EVOLUTION_API_KEY`, `EVOLUTION_INSTANCE`.

## Link WhatsApp (once)

1. Open http://127.0.0.1:8080/manager and log in with `EVOLUTION_API_KEY` from `.env`.
2. Create an instance named exactly as `EVOLUTION_INSTANCE` (default `notifier`), engine Baileys.
3. Click connect and scan the QR code with WhatsApp (Linked devices).

## Find recipient IDs

- Telegram: `GET http://127.0.0.1:8000/telegram/chats`. Message the bot first (in groups send `/start@<bot>`); only the last ~24 h are visible. If `migrated_to` is set, use that ID.
- WhatsApp groups: `GET http://127.0.0.1:8000/whatsapp/groups`, use `group_id`. Individual chats: phone number with country code, digits only.

## Publish

Exchange `notifications`, persistent messages, JSON body.

Telegram, routing key `telegram.send`:

    {"id": "<uuid>", "version": 1, "channel": "telegram",
     "recipient": {"chat_id": -1004317452395}, "text": "<b>Hi</b>", "parse_mode": "HTML"}

WhatsApp, routing key `whatsapp.send`:

    {"id": "<uuid>", "version": 1, "channel": "whatsapp",
     "recipient": {"to": "120363295648424210@g.us"}, "text": "*Hi*"}

Use a new `id` per notification; repeated ids are skipped as duplicates.

Failed messages land in `notify.dlq` (Telegram) or `notify.whatsapp.dlq` (WhatsApp) with the reason in the `x-error` header.

## Develop

    uv sync
    docker compose up -d rabbitmq
    uv run pytest
```

- [ ] **Step 4: Validate compose and bring the stack up**

```bash
docker compose config -q && echo COMPOSE_OK
docker compose up -d --build --remove-orphans
docker compose ps --format '{{.Service}} {{.State}} {{.Health}}'
```
Expected: `COMPOSE_OK`; all services `running` (rabbitmq and evolution-postgres `healthy`). `docker compose logs evolution-api | tail -20` shows the server listening on 8080 without database errors.

- [ ] **Step 5: Smoke the API**

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/telegram/chats
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/whatsapp/groups
```
Expected: `{"status":"ok"}`; a JSON list containing chat `-1004317452395`; WhatsApp returns `502` until an instance exists and is connected (Task 7).

---

### Task 7: Live WhatsApp verification

Needs the user to scan a QR code. Everything else is scripted.

- [ ] **Step 1: Create the instance via API and show the QR**

```bash
KEY=$(sed -n 's/^EVOLUTION_API_KEY=//p' .env | tr -d '\r')
INST=$(sed -n 's/^EVOLUTION_INSTANCE=//p' .env | tr -d '\r')
curl -s -X POST http://127.0.0.1:8080/instance/create -H "apikey: $KEY" -H 'content-type: application/json' \
  -d "{\"instanceName\":\"$INST\",\"integration\":\"WHATSAPP-BAILEYS\",\"qrcode\":true}" | head -c 300
```
Then ask the user to open http://127.0.0.1:8080/manager, log in with the key (tell them where it is in `.env`, do not print it), and scan the QR for the instance.

- [ ] **Step 2: Confirm connection**

```bash
curl -s http://127.0.0.1:8080/instance/connectionState/$INST -H "apikey: $KEY"
```
Expected: `"state":"open"`.

- [ ] **Step 3: List groups through our API**

Run: `curl -s http://127.0.0.1:8000/whatsapp/groups`
Expected: JSON list of `{group_id, name, size, announce}`.

- [ ] **Step 4: Send to a group and to a number**

Publish one `whatsapp.send` envelope to a group from Step 3 and one to the user's own number (ask for it), via the RabbitMQ management API as in the README. Check `docker compose logs whatsapp-worker` shows `"outcome": "sent"` with a `message_id`, and the user confirms both arrived.

- [ ] **Step 5: Verify the disconnected-session reply (Review Focus 1)**

```bash
curl -s -X DELETE http://127.0.0.1:8080/instance/logout/$INST -H "apikey: $KEY"
```
**Ask the user before running this**: it unlinks WhatsApp and needs a new QR scan afterwards. If they agree, publish one message, read the `error` in `docker compose logs whatsapp-worker`. Expected: `"outcome": "retry"`. If it is `dead_letter`, add the exact phrase to the retryable check in `classify_evolution` with a test (RED then GREEN), then re-link by QR. If the user declines, record that the disconnected case is unverified.

- [ ] **Step 6: Confirm no secrets in logs**

```bash
TG=$(sed -n 's/^TELEGRAM_BOT_TOKEN=//p' .env | tr -d '\r"'"'"); EK=$(sed -n 's/^EVOLUTION_API_KEY=//p' .env | tr -d '\r')
docker compose logs telegram-worker whatsapp-worker api 2>&1 | grep -cF -e "$TG" -e "$EK"
```
Expected: `0`.
