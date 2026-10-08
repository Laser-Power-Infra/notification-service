import json
import uuid

import httpx
import pytest

from notifier.envelope import WhatsAppEnvelope, WhatsAppRecipient
from notifier.results import Ok, Permanent, Retryable
from notifier.whatsapp import EvolutionClient, classify_evolution

KEY = "EVO-SECRET-KEY"


def env(to="+919876543210"):
    return WhatsAppEnvelope(id=uuid.uuid4(), version=1, notification_platform="whatsapp",
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
