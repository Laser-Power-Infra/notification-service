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
