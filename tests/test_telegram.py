import json
import logging
import uuid

import httpx
import pytest

from notifier.envelope import TelegramEnvelope as Envelope, TelegramRecipient as Recipient
from notifier.telegram import Ok, Permanent, Retryable, TelegramClient, classify

TOKEN = "123456:SECRET-TOKEN"


def env(**kw):
    return Envelope(
        id=uuid.uuid4(), version=1, notification_platform="telegram",
        recipient=Recipient(chat_id=42), text="hello", **kw,
    )


def client(handler):
    return TelegramClient(TOKEN, httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_sends_expected_request():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True, "result": {}})

    result = await client(handler).send(env(parse_mode="HTML"))
    assert result == Ok()
    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == f"/bot{TOKEN}/sendMessage"
    assert json.loads(req.content) == {
        "chat_id": 42, "text": "hello", "disable_notification": False, "parse_mode": "HTML",
    }


async def test_parse_mode_omitted_when_none():
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    await client(handler).send(env())
    assert "parse_mode" not in seen[0]


@pytest.mark.parametrize(
    "status, body, expected",
    [
        (200, {"ok": True}, Ok()),
        (200, {"ok": True, "result": {"message_id": 55}}, Ok("55")),
        (400, {"ok": False, "description": "Bad Request: chat not found"},
         Permanent("400 Bad Request: chat not found")),
        (403, {"ok": False, "description": "Forbidden: bot was blocked by the user"},
         Permanent("403 Forbidden: bot was blocked by the user")),
        (429, {"ok": False, "description": "Too Many Requests", "parameters": {"retry_after": 7}},
         Retryable("429 Too Many Requests", 7.0)),
        (429, {"ok": False}, Retryable("429", None)),
        (500, {"ok": False, "description": "Internal"}, Retryable("500 Internal")),
        (502, None, Retryable("502")),
        (401, {"ok": False, "description": "Unauthorized"}, Retryable("401 Unauthorized")),
        (200, {"ok": False}, Retryable("200")),
    ],
)
def test_classify(status, body, expected):
    assert classify(status, body) == expected


async def test_non_json_body_is_retryable():
    def handler(request):
        return httpx.Response(502, text="<html>Bad Gateway</html>")

    assert await client(handler).send(env()) == Retryable("502")


async def test_network_error_is_retryable_without_token():
    def handler(request):
        raise httpx.ConnectError(f"cannot reach {request.url}")

    result = await client(handler).send(env())
    assert isinstance(result, Retryable)
    assert result.reason == "network error: ConnectError"
    assert TOKEN not in result.reason


async def test_token_not_logged(caplog):
    caplog.set_level(logging.DEBUG)

    def handler(request):
        return httpx.Response(200, json={"ok": True})

    await client(handler).send(env())
    assert TOKEN not in caplog.text


async def test_non_http_error_is_retryable():
    # A token with a stray newline (e.g. from an .env file) makes httpx raise InvalidURL,
    # which is not an httpx.HTTPError.
    tg = TelegramClient("123:ABC\n", httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))))
    result = await tg.send(env())
    assert result == Retryable("network error: InvalidURL")
