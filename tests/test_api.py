import json

import httpx
import pytest

from notifier.api import NotRouted, create_app, telegram_chats
from notifier.config import ApiSettings
from notifier.envelope import parse_envelope

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
    base = dict(amqp_url="amqp://x/", telegram_bot_token=TOKEN, evolution_url="http://evo:8080",
                evolution_api_key=KEY, evolution_instance="inst")
    base.update(kw)
    return ApiSettings(**base)


def api(handler, publish=None, **kw):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(settings(**kw), http=http, publish=publish)
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


class FakePublish:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    async def __call__(self, routing_key, body):
        if self.error:
            raise self.error
        self.calls.append((routing_key, body))


@pytest.mark.parametrize("path, payload, platform", [
    ("/telegram/test", {"chat_id": -1004317452395, "text": "<b>hi</b>", "parse_mode": "HTML"}, "telegram"),
    ("/whatsapp/test", {"to": "+919876543210", "text": "*hi*"}, "whatsapp"),
])
async def test_test_route_publishes_valid_envelope(path, payload, platform):
    pub = FakePublish()
    async with api(lambda r: httpx.Response(500), publish=pub) as c:
        r = await c.post(path, json=payload)
    assert r.status_code == 202
    routing_key, body = pub.calls[0]
    env = parse_envelope(body)
    assert routing_key == platform and env.notification_platform == platform
    assert r.json() == {"id": str(env.id), "notification_platform": platform}
    assert env.text == payload["text"]


async def test_test_route_default_text():
    pub = FakePublish()
    async with api(lambda r: httpx.Response(500), publish=pub) as c:
        r = await c.post("/whatsapp/test", json={"to": "120363295648424210@g.us"})
    assert r.status_code == 202
    assert json.loads(pub.calls[0][1])["text"].startswith("Test message from notifier")


@pytest.mark.parametrize("path, payload", [
    ("/telegram/test", {"chat_id": "abc"}),
    ("/telegram/test", {}),
    ("/telegram/test", {"chat_id": 1, "text": "   "}),
    ("/whatsapp/test", {"to": "+91 98765-43210"}),
    ("/whatsapp/test", {"to": "919876543210", "text": "x" * 65537}),
])
async def test_test_route_rejects_bad_input(path, payload):
    pub = FakePublish()
    async with api(lambda r: httpx.Response(500), publish=pub) as c:
        r = await c.post(path, json=payload)
    assert r.status_code == 422 and pub.calls == []


@pytest.mark.parametrize("publish, kw, status, detail", [
    (FakePublish(), {"amqp_url": None}, 503, "rabbitmq not configured"),
    (FakePublish(NotRouted()), {}, 503, "rabbitmq: message not routed (is the notifier worker running?)"),
    (FakePublish(ConnectionRefusedError("amqp://user:pass@host")), {}, 502, "rabbitmq: ConnectionRefusedError"),
])
async def test_test_route_errors(publish, kw, status, detail):
    async with api(lambda r: httpx.Response(500), publish=publish, **kw) as c:
        r = await c.post("/telegram/test", json={"chat_id": 1})
    assert r.status_code == status and r.json() == {"detail": detail}
