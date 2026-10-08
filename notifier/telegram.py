import httpx

from notifier.envelope import TelegramEnvelope
from notifier.results import Ok, Permanent, Retryable, SendResult


def classify(status: int, body: object) -> SendResult:
    body = body if isinstance(body, dict) else {}
    reason = f"{status} {str(body.get('description', ''))[:200]}".strip()
    if status == 200 and body.get("ok") is True:
        result = body.get("result")
        message_id = result.get("message_id") if isinstance(result, dict) else None
        return Ok(str(message_id) if message_id is not None else None)
    if status == 429:
        params = body.get("parameters")
        retry_after = params.get("retry_after") if isinstance(params, dict) else None
        return Retryable(reason, float(retry_after) if isinstance(retry_after, (int, float)) else None)
    if status in (400, 403):
        return Permanent(reason)
    # 5xx, 401/404 (bad token: retry so a config fix can still deliver), anything unexpected.
    return Retryable(reason)


class TelegramClient:
    def __init__(self, token: str, http: httpx.AsyncClient, base_url: str = "https://api.telegram.org"):
        self._url = f"{base_url}/bot{token}/sendMessage"
        self._http = http

    async def send(self, env: TelegramEnvelope) -> SendResult:
        payload: dict[str, object] = {
            "chat_id": env.recipient.chat_id,
            "text": env.text,
            "disable_notification": env.disable_notification,
        }
        if env.parse_mode:
            payload["parse_mode"] = env.parse_mode
        try:
            resp = await self._http.post(self._url, json=payload)
        except Exception as e:  # HTTPError, plus InvalidURL etc. (e.g. token with a stray newline)
            # Only the type name: exception text can contain the URL, which contains the token.
            return Retryable(f"network error: {type(e).__name__}")
        try:
            body = resp.json()
        except ValueError:
            body = None
        return classify(resp.status_code, body)
