# Telegram Notification Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single async Python service that consumes notification envelopes from RabbitMQ and delivers them to Telegram, with RabbitMQ-native retries and a dead-letter queue.

**Architecture:** One asyncio process. `aio-pika` consumes the quorum queue `notify.telegram` with manual acks. Each message is validated, deduplicated, rate-limited, and sent through the Telegram Bot API with `httpx`. A pure `decide()` function maps the send result to ack, retry (publish to a TTL queue that dead-letters back), or DLQ. The original message is acked only after the retry/DLQ copy is confirmed by the broker.

**Tech Stack:** Python 3.12, uv, aio-pika, httpx, pydantic v2, pydantic-settings, pytest, pytest-asyncio, RabbitMQ 4 (Docker).

**Spec:** `docs/superpowers/specs/2026-10-01-telegram-notification-service-design.md`

## Global Constraints

- Packages are managed only with `uv`. Never use `pip`, `poetry`, or `requirements.txt`. Add packages with `uv add` / `uv add --dev`, run everything with `uv run`.
- Python version 3.12, pinned in `.python-version`.
- **No git.** The user asked for no `git init` and no commits. Do not run any git command. Pass `--vcs none` to `uv init`.
- Runtime dependencies: `aio-pika`, `httpx`, `pydantic`, `pydantic-settings` only. Dev dependencies: `pytest`, `pytest-asyncio` only. No Telegram SDK.
- `TELEGRAM_BOT_TOKEN` must never appear in logs or error reasons.
- Envelope `text` is 1 to 4096 characters. Unknown envelope fields are rejected.
- Rate limits: 25 messages per second overall, 1 message per second per chat.
- Default retry delays `10,60,300` seconds. Retry queues are named `notify.retry.<seconds>s`.
- Queue and exchange names: exchange `notifications` (topic), routing key `telegram.send`, main queue `notify.telegram` (quorum), DLQ `notify.dlq`.
- Inline 429 wait is capped at 60 seconds; longer waits use the retry queue.
- Dedup cache holds the last 10,000 sent ids, in memory.

## Review Focus

1. **Bot token leaking into logs.** `httpx` logs every request URL at INFO, and the URL contains the token. Expect: token never appears in any log line or `x-error` header. Test in Task 3.
2. **Same `id` published twice at nearly the same time** (producer retry). With prefetch 10, both copies are processed concurrently and both pass a naive "already sent?" check. Expect: Telegram is called once. Test in Task 5.
3. **Garbage `x-attempt` header** (string, zero, negative, huge float). Expect: treated as attempt 1, no crash, no requeue loop. Test in Task 5.
4. **Body that is not an envelope** (non-UTF-8 bytes, a JSON array, whitespace-only `text`). Expect: goes to DLQ with a readable reason, never crashes the handler. Tests in Tasks 2 and 5.
5. **Telegram returns a non-JSON body** (HTML 502 page from a proxy) **or 401** (bad token). Expect: retryable, no exception. Test in Task 3.

---

## File Structure

```
.python-version          # 3.12 (created by uv init)
pyproject.toml           # project metadata, deps (via uv add), pytest config
uv.lock                  # created by uv
.dockerignore
Dockerfile
docker-compose.yml
notifier/__init__.py     # empty
notifier/config.py       # Settings (env vars)
notifier/envelope.py     # Envelope pydantic model
notifier/telegram.py     # TelegramClient, Ok/Retryable/Permanent, classify()
notifier/ratelimit.py    # RateLimiter
notifier/handler.py      # decide(), parse_attempt(), RecentIds, Handler
notifier/topology.py     # queue names, declare(), Publisher
notifier/main.py         # logging setup, run(), main()
tests/test_config.py
tests/test_envelope.py
tests/test_telegram.py
tests/test_ratelimit.py
tests/test_handler.py
tests/test_integration.py
```

---

### Task 1: Project scaffold and config

**Files:**
- Create (via uv): `pyproject.toml`, `.python-version`, `uv.lock`
- Create: `notifier/__init__.py`, `notifier/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `notifier.config.Settings` with fields `amqp_url: str`, `telegram_bot_token: SecretStr`, `prefetch: int = 10`, `retry_delays: list[int] = [10, 60, 300]`, `log_level: str = "INFO"`.

- [ ] **Step 1: Initialise the project with uv**

Run in `D:\Projects\notification-service`:
```bash
uv init --no-package --vcs none --python 3.12 --name notifier
```
Then delete the generated `main.py` and `README.md` at the repo root (they are uv's hello-world stubs).

- [ ] **Step 2: Add dependencies**

```bash
uv add aio-pika httpx pydantic pydantic-settings
uv add --dev pytest pytest-asyncio
```
Expected: `pyproject.toml` lists them and `uv.lock` exists.

- [ ] **Step 3: Add pytest config to `pyproject.toml`**

Append:
```toml
[tool.pytest.ini_options]
pythonpath = ["."]
asyncio_mode = "auto"
markers = ["integration: needs RabbitMQ at AMQP_URL (docker compose up -d rabbitmq)"]
```

- [ ] **Step 4: Write the failing test**

`notifier/__init__.py`: empty file.

`tests/test_config.py`:
```python
import pytest
from pydantic import ValidationError

from notifier.config import Settings


def test_defaults(monkeypatch):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:secret")
    s = Settings()
    assert s.prefetch == 10
    assert s.retry_delays == [10, 60, 300]
    assert s.log_level == "INFO"
    assert s.telegram_bot_token.get_secret_value() == "123:secret"


def test_retry_delays_from_comma_string(monkeypatch):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("RETRY_DELAYS", "5, 30")
    assert Settings().retry_delays == [5, 30]


@pytest.mark.parametrize("bad", ["", "0", "-5", "10,10"])
def test_retry_delays_rejects_bad_values(monkeypatch, bad):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("RETRY_DELAYS", bad)
    with pytest.raises(ValidationError):
        Settings()


def test_token_hidden_in_repr(monkeypatch):
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:secret")
    assert "123:secret" not in repr(Settings())
```

- [ ] **Step 5: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.config'`.

- [ ] **Step 6: Implement `notifier/config.py`**

```python
from typing import Annotated, Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode


class Settings(BaseSettings):
    amqp_url: str
    telegram_bot_token: SecretStr
    prefetch: int = Field(10, ge=1)
    retry_delays: Annotated[list[int], NoDecode] = [10, 60, 300]
    log_level: str = "INFO"

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

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/test_config.py -v`
Expected: all PASS.

---

### Task 2: Envelope model

**Files:**
- Create: `notifier/envelope.py`
- Test: `tests/test_envelope.py`

**Interfaces:**
- Produces: `notifier.envelope.Envelope` (fields `id: UUID`, `version: Literal[1]`, `channel: Literal["telegram"]`, `recipient: Recipient`, `text: str`, `parse_mode: Literal["HTML", "MarkdownV2"] | None`, `disable_notification: bool`), `notifier.envelope.Recipient` (`chat_id: int`). Parse with `Envelope.model_validate_json(body: bytes)`, which raises `pydantic.ValidationError`.

- [ ] **Step 1: Write the failing test**

`tests/test_envelope.py`:
```python
import json
import uuid

import pytest
from pydantic import ValidationError

from notifier.envelope import Envelope


def body(**overrides):
    data = {
        "id": str(uuid.uuid4()),
        "version": 1,
        "channel": "telegram",
        "recipient": {"chat_id": -1001234567890},
        "text": "<b>hi</b>",
        "parse_mode": "HTML",
    }
    data.update(overrides)
    return json.dumps(data).encode()


def test_valid_envelope():
    env = Envelope.model_validate_json(body())
    assert env.recipient.chat_id == -1001234567890
    assert env.parse_mode == "HTML"
    assert env.disable_notification is False


def test_optional_fields_absent():
    data = json.loads(body())
    del data["parse_mode"]
    env = Envelope.model_validate_json(json.dumps(data))
    assert env.parse_mode is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"version": 2},
        {"channel": "email"},
        {"text": ""},
        {"text": "   \n "},
        {"text": "x" * 4097},
        {"parse_mode": "Markdown"},
        {"recipient": {"chat_id": "abc"}},
        {"recipient": {"chat_id": 1, "extra": 1}},
        {"id": "not-a-uuid"},
        {"unknown_field": 1},
    ],
)
def test_invalid_envelopes(overrides):
    with pytest.raises(ValidationError):
        Envelope.model_validate_json(body(**overrides))


@pytest.mark.parametrize("raw", [b"\xff\xfe", b"[]", b"not json", b""])
def test_non_envelope_bodies(raw):
    with pytest.raises(ValidationError):
        Envelope.model_validate_json(raw)


def test_max_length_text_ok():
    assert len(Envelope.model_validate_json(body(text="x" * 4096)).text) == 4096
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.envelope'`.

- [ ] **Step 3: Implement `notifier/envelope.py`**

```python
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator


class Recipient(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chat_id: StrictInt


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    version: Literal[1]
    channel: Literal["telegram"]
    recipient: Recipient
    text: str = Field(min_length=1, max_length=4096)
    parse_mode: Literal["HTML", "MarkdownV2"] | None = None
    disable_notification: bool = False

    @field_validator("text")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        # Telegram rejects whitespace-only text with 400; fail early with a clear reason.
        if not v.strip():
            raise ValueError("text must not be blank")
        return v
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: all PASS.

---

### Task 3: Telegram client

**Files:**
- Create: `notifier/telegram.py`
- Test: `tests/test_telegram.py`

**Interfaces:**
- Consumes: `Envelope` from Task 2.
- Produces:
  - `Ok()`, `Retryable(reason: str, retry_after: float | None = None)`, `Permanent(reason: str)` frozen dataclasses; `SendResult = Ok | Retryable | Permanent`.
  - `classify(status: int, body: object) -> SendResult`.
  - `TelegramClient(token: str, http: httpx.AsyncClient, base_url: str = "https://api.telegram.org")` with `async send(env: Envelope) -> SendResult`. Never raises for HTTP or network errors.

- [ ] **Step 1: Write the failing test**

`tests/test_telegram.py`:
```python
import json
import logging
import uuid

import httpx
import pytest

from notifier.envelope import Envelope, Recipient
from notifier.telegram import Ok, Permanent, Retryable, TelegramClient, classify

TOKEN = "123456:SECRET-TOKEN"


def env(**kw):
    return Envelope(
        id=uuid.uuid4(), version=1, channel="telegram",
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_telegram.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.telegram'`.

- [ ] **Step 3: Implement `notifier/telegram.py`**

```python
import logging
from dataclasses import dataclass

import httpx

from notifier.envelope import Envelope

# httpx logs every request URL at INFO, and the URL contains the bot token.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


@dataclass(frozen=True)
class Ok:
    pass


@dataclass(frozen=True)
class Retryable:
    reason: str
    retry_after: float | None = None


@dataclass(frozen=True)
class Permanent:
    reason: str


SendResult = Ok | Retryable | Permanent


def classify(status: int, body: object) -> SendResult:
    body = body if isinstance(body, dict) else {}
    reason = f"{status} {str(body.get('description', ''))[:200]}".strip()
    if status == 200 and body.get("ok") is True:
        return Ok()
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

    async def send(self, env: Envelope) -> SendResult:
        payload: dict[str, object] = {
            "chat_id": env.recipient.chat_id,
            "text": env.text,
            "disable_notification": env.disable_notification,
        }
        if env.parse_mode:
            payload["parse_mode"] = env.parse_mode
        try:
            resp = await self._http.post(self._url, json=payload)
        except httpx.HTTPError as e:
            # Only the type name: exception text can contain the URL, which contains the token.
            return Retryable(f"network error: {type(e).__name__}")
        try:
            body = resp.json()
        except ValueError:
            body = None
        return classify(resp.status_code, body)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_telegram.py -v`
Expected: all PASS.

---

### Task 4: Rate limiter

**Files:**
- Create: `notifier/ratelimit.py`
- Test: `tests/test_ratelimit.py`

**Interfaces:**
- Produces: `RateLimiter(global_per_sec: float = 25.0, per_chat_interval: float = 1.0, *, clock=time.monotonic, sleep=asyncio.sleep)` with `async acquire(chat_id: int) -> None`. Waits until both the chat slot and a global slot are free.

- [ ] **Step 1: Write the failing test**

`tests/test_ratelimit.py`:
```python
import pytest

from notifier.ratelimit import RateLimiter


class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def limiter(clock):
    return RateLimiter(25.0, 1.0, clock=clock, sleep=clock.sleep)


async def test_first_call_does_not_wait():
    c = FakeClock()
    await limiter(c).acquire(1)
    assert c.sleeps == []


async def test_same_chat_waits_one_second():
    c = FakeClock()
    rl = limiter(c)
    await rl.acquire(1)
    await rl.acquire(1)
    assert c.sleeps == [pytest.approx(1.0)]


async def test_different_chats_only_spaced_by_global_limit():
    c = FakeClock()
    rl = limiter(c)
    await rl.acquire(1)
    await rl.acquire(2)
    assert c.sleeps == [pytest.approx(0.04)]


async def test_no_wait_after_time_passes():
    c = FakeClock()
    rl = limiter(c)
    await rl.acquire(1)
    c.now += 5
    await rl.acquire(1)
    assert c.sleeps == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_ratelimit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.ratelimit'`.

- [ ] **Step 3: Implement `notifier/ratelimit.py`**

```python
import asyncio
import time


class RateLimiter:
    """Reserve a per-chat slot first, then a global slot, so a busy chat does not block other chats."""

    def __init__(self, global_per_sec: float = 25.0, per_chat_interval: float = 1.0, *,
                 clock=time.monotonic, sleep=asyncio.sleep):
        self._global_interval = 1.0 / global_per_sec
        self._per_chat_interval = per_chat_interval
        self._next_global = 0.0
        self._next_chat: dict[int, float] = {}
        self._clock = clock
        self._sleep = sleep

    async def acquire(self, chat_id: int) -> None:
        # Read-and-update has no await in between, so it is atomic under asyncio.
        now = self._clock()
        start = max(now, self._next_chat.get(chat_id, now))
        self._next_chat[chat_id] = start + self._per_chat_interval
        if start > now:
            await self._sleep(start - now)

        now = self._clock()
        start = max(now, self._next_global)
        self._next_global = start + self._global_interval
        if start > now:
            await self._sleep(start - now)

        # ponytail: in-memory, single process only; move to Redis if we run several instances.
        if len(self._next_chat) > 10_000:
            self._next_chat = {c: t for c, t in self._next_chat.items() if t > now}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_ratelimit.py -v`
Expected: all PASS.

---

### Task 5: Handler (decision, dedup, per-message flow)

**Files:**
- Create: `notifier/handler.py`
- Test: `tests/test_handler.py`

**Interfaces:**
- Consumes: `Envelope` (Task 2); `Ok`, `Retryable`, `Permanent`, `SendResult`, and any object with `async send(env) -> SendResult` (Task 3); any object with `async acquire(chat_id)` (Task 4).
- Produces:
  - `Ack()`, `Retry(queue: str, attempt: int, reason: str)`, `DeadLetter(reason: str)` frozen dataclasses.
  - `decide(result: SendResult, attempt: int, retry_queues: list[str]) -> Ack | Retry | DeadLetter`.
  - `parse_attempt(headers: Mapping[str, Any]) -> int`.
  - `RecentIds(maxsize: int)` with `__contains__` and `add`.
  - `Handler(telegram, limiter, publisher, retry_queues: list[str], *, dedup_size=10_000, max_inline_wait=60.0, sleep=asyncio.sleep)` with `async handle(body: bytes, headers: Mapping[str, Any]) -> None`. Returning normally means "ack the original". Raising means "requeue". The `publisher` must provide `async retry(body, headers, queue, attempt)` and `async dead_letter(body, headers, reason)` (implemented in Task 6).

- [ ] **Step 1: Write the failing test**

`tests/test_handler.py`:
```python
import asyncio
import json
import uuid

import pytest

from notifier.handler import Ack, DeadLetter, Handler, RecentIds, Retry, decide, parse_attempt
from notifier.telegram import Ok, Permanent, Retryable

QUEUES = ["notify.retry.10s", "notify.retry.60s", "notify.retry.300s"]


def body(id=None, text="hi"):
    return json.dumps({
        "id": str(id or uuid.uuid4()), "version": 1, "channel": "telegram",
        "recipient": {"chat_id": 7}, "text": text,
    }).encode()


class FakeTelegram:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    async def send(self, env):
        self.calls += 1
        await asyncio.sleep(0)  # yield so concurrent handlers interleave
        return self.results.pop(0) if self.results else Ok()


class FakeLimiter:
    async def acquire(self, chat_id):
        pass


class FakePublisher:
    def __init__(self):
        self.retries = []
        self.dead = []

    async def retry(self, body, headers, queue, attempt):
        self.retries.append((queue, attempt))

    async def dead_letter(self, body, headers, reason):
        self.dead.append(reason)


async def no_sleep(seconds):
    no_sleep.calls.append(seconds)
no_sleep.calls = []


def make(*results):
    tg, pub = FakeTelegram(*results), FakePublisher()
    no_sleep.calls = []
    return Handler(tg, FakeLimiter(), pub, QUEUES, sleep=no_sleep), tg, pub


# --- decide ---

def test_decide_ok_acks():
    assert decide(Ok(), 1, QUEUES) == Ack()


def test_decide_permanent_dead_letters():
    assert decide(Permanent("400 x"), 1, QUEUES) == DeadLetter("400 x")


@pytest.mark.parametrize("attempt, queue", [(1, QUEUES[0]), (2, QUEUES[1]), (3, QUEUES[2])])
def test_decide_retry_picks_queue_by_attempt(attempt, queue):
    assert decide(Retryable("500"), attempt, QUEUES) == Retry(queue, attempt + 1, "500")


def test_decide_retries_exhausted():
    assert decide(Retryable("500"), 4, QUEUES) == DeadLetter("retries exhausted: 500")


# --- parse_attempt ---

@pytest.mark.parametrize("headers, expected", [
    ({}, 1), ({"x-attempt": 3}, 3), ({"x-attempt": "2"}, 2),
    ({"x-attempt": "abc"}, 1), ({"x-attempt": 0}, 1), ({"x-attempt": -4}, 1),
    ({"x-attempt": float("inf")}, 1), ({"x-attempt": None}, 1),
])
def test_parse_attempt(headers, expected):
    assert parse_attempt(headers) == expected


# --- RecentIds ---

def test_recent_ids_evicts_oldest():
    ids = RecentIds(2)
    ids.add("a"); ids.add("b"); ids.add("c")
    assert "a" not in ids and "b" in ids and "c" in ids


# --- Handler ---

async def test_success_sends_once_and_acks():
    h, tg, pub = make(Ok())
    await h.handle(body(), {})
    assert tg.calls == 1 and pub.retries == [] and pub.dead == []


async def test_duplicate_id_is_skipped():
    h, tg, pub = make()
    i = uuid.uuid4()
    await h.handle(body(i), {})
    await h.handle(body(i), {})
    assert tg.calls == 1


async def test_concurrent_duplicates_send_once():
    h, tg, pub = make()
    i = uuid.uuid4()
    await asyncio.gather(h.handle(body(i), {}), h.handle(body(i), {}))
    assert tg.calls == 1


async def test_failed_id_is_not_marked_sent():
    h, tg, pub = make(Retryable("500"), Ok())
    i = uuid.uuid4()
    await h.handle(body(i), {})
    await h.handle(body(i), {"x-attempt": 2})
    assert tg.calls == 2


async def test_retryable_publishes_to_retry_queue():
    h, tg, pub = make(Retryable("500"))
    await h.handle(body(), {"x-attempt": 2})
    assert pub.retries == [(QUEUES[1], 3)]


async def test_garbage_attempt_header_treated_as_first():
    h, tg, pub = make(Retryable("500"))
    await h.handle(body(), {"x-attempt": "garbage"})
    assert pub.retries == [(QUEUES[0], 2)]


async def test_permanent_goes_to_dlq():
    h, tg, pub = make(Permanent("403 Forbidden"))
    await h.handle(body(), {})
    assert pub.dead == ["403 Forbidden"]


@pytest.mark.parametrize("raw", [b"\xff\xfe", b"[]", b"{}", body(text="   ")])
async def test_invalid_body_goes_to_dlq_without_sending(raw):
    h, tg, pub = make()
    await h.handle(raw, {})
    assert tg.calls == 0
    assert len(pub.dead) == 1 and pub.dead[0].startswith("invalid envelope:")


async def test_short_429_waits_inline_and_resends():
    h, tg, pub = make(Retryable("429", 5.0), Ok())
    await h.handle(body(), {})
    assert no_sleep.calls == [5.0] and tg.calls == 2 and pub.retries == []


async def test_long_429_uses_retry_queue():
    h, tg, pub = make(Retryable("429", 120.0))
    await h.handle(body(), {})
    assert no_sleep.calls == [] and tg.calls == 1 and pub.retries == [(QUEUES[0], 2)]


async def test_publisher_failure_propagates_for_requeue():
    h, tg, pub = make(Retryable("500"))

    async def boom(*a):
        raise ConnectionError("broker gone")
    pub.retry = boom
    with pytest.raises(ConnectionError):
        await h.handle(body(), {})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_handler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.handler'`.

- [ ] **Step 3: Implement `notifier/handler.py`**

```python
import asyncio
import logging
from collections import OrderedDict
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from notifier.envelope import Envelope
from notifier.telegram import Ok, Permanent, Retryable, SendResult

log = logging.getLogger("notifier")


@dataclass(frozen=True)
class Ack:
    pass


@dataclass(frozen=True)
class Retry:
    queue: str
    attempt: int  # attempt number the republished copy will carry
    reason: str


@dataclass(frozen=True)
class DeadLetter:
    reason: str


Action = Ack | Retry | DeadLetter


def decide(result: SendResult, attempt: int, retry_queues: list[str]) -> Action:
    if isinstance(result, Ok):
        return Ack()
    if isinstance(result, Permanent):
        return DeadLetter(result.reason)
    if attempt <= len(retry_queues):
        return Retry(retry_queues[attempt - 1], attempt + 1, result.reason)
    return DeadLetter(f"retries exhausted: {result.reason}")


def parse_attempt(headers: Mapping[str, Any]) -> int:
    try:
        n = int(headers.get("x-attempt", 1))
    except (TypeError, ValueError, OverflowError):
        return 1
    return n if n >= 1 else 1


class RecentIds:
    def __init__(self, maxsize: int):
        self._maxsize = maxsize
        self._ids: OrderedDict[Hashable, None] = OrderedDict()

    def __contains__(self, key: Hashable) -> bool:
        return key in self._ids

    def add(self, key: Hashable) -> None:
        self._ids[key] = None
        self._ids.move_to_end(key)
        if len(self._ids) > self._maxsize:
            self._ids.popitem(last=False)


def _summarize(e: ValidationError) -> str:
    # loc + msg only: never echo input values (could be large or personal data).
    return "; ".join(f"{'.'.join(map(str, err['loc'])) or 'body'}: {err['msg']}" for err in e.errors()[:3])


class Handler:
    def __init__(self, telegram, limiter, publisher, retry_queues: list[str], *,
                 dedup_size: int = 10_000, max_inline_wait: float = 60.0, sleep=asyncio.sleep):
        self._telegram = telegram
        self._limiter = limiter
        self._publisher = publisher
        self._retry_queues = retry_queues
        self._max_inline_wait = max_inline_wait
        self._sleep = sleep
        # ponytail: in-memory dedup, lost on restart; persist in SQLite in phase 2.
        self._sent = RecentIds(dedup_size)
        self._inflight: set[UUID] = set()

    async def handle(self, body: bytes, headers: Mapping[str, Any]) -> None:
        attempt = parse_attempt(headers)
        try:
            env = Envelope.model_validate_json(body)
        except ValidationError as e:
            reason = f"invalid envelope: {_summarize(e)}"
            await self._publisher.dead_letter(body, headers, reason)
            _log("dead_letter", None, None, attempt, reason)
            return

        chat_id = env.recipient.chat_id
        if env.id in self._sent or env.id in self._inflight:
            _log("duplicate", env.id, chat_id, attempt)
            return

        self._inflight.add(env.id)
        try:
            action = decide(await self._send(env), attempt, self._retry_queues)
            if isinstance(action, Ack):
                self._sent.add(env.id)
                _log("sent", env.id, chat_id, attempt)
            elif isinstance(action, Retry):
                await self._publisher.retry(body, headers, action.queue, action.attempt)
                _log("retry", env.id, chat_id, attempt, action.reason)
            else:
                await self._publisher.dead_letter(body, headers, action.reason)
                _log("dead_letter", env.id, chat_id, attempt, action.reason)
        finally:
            self._inflight.discard(env.id)

    async def _send(self, env: Envelope) -> SendResult:
        await self._limiter.acquire(env.recipient.chat_id)
        result = await self._telegram.send(env)
        if (isinstance(result, Retryable) and result.retry_after is not None
                and result.retry_after <= self._max_inline_wait):
            await self._sleep(result.retry_after)
            await self._limiter.acquire(env.recipient.chat_id)
            result = await self._telegram.send(env)
        return result


def _log(outcome: str, id: UUID | None, chat_id: int | None, attempt: int, error: str | None = None) -> None:
    fields = {"outcome": outcome, "id": id, "chat_id": chat_id, "attempt": attempt}
    if error:
        fields["error"] = error
    log.info("notification", extra={"fields": fields})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_handler.py -v`
Expected: all PASS.

- [ ] **Step 5: Run the whole unit suite**

Run: `uv run pytest -v`
Expected: all PASS (no integration tests exist yet).

---

### Task 6: Topology, publisher, entrypoint, integration tests

**Files:**
- Create: `notifier/topology.py`, `notifier/main.py`, `docker-compose.yml`
- Test: `tests/test_integration.py`

**Interfaces:**
- Consumes: `Settings` (Task 1), `TelegramClient` (Task 3), `RateLimiter` (Task 4), `Handler` (Task 5).
- Produces:
  - `topology`: constants `EXCHANGE = "notifications"`, `ROUTING_KEY = "telegram.send"`, `MAIN_QUEUE = "notify.telegram"`, `DLQ_QUEUE = "notify.dlq"`; `retry_queue_name(delay: int) -> str`; `async declare(channel, retry_delays: list[int]) -> tuple[AbstractQueue, list[str]]`; `Publisher(channel)` with `async retry(body, headers, queue, attempt)` and `async dead_letter(body, headers, reason)`.
  - `main`: `async run(settings: Settings, http: httpx.AsyncClient | None = None, stop: asyncio.Event | None = None) -> None`; `main() -> None`; `JsonFormatter`.

- [ ] **Step 1: Create `docker-compose.yml` (RabbitMQ only for now)**

The default `guest` user can only log in from localhost, so a dedicated user is set. That user also works from other containers in Task 7.
```yaml
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
```
Run: `docker compose up -d rabbitmq` and wait until `docker compose ps` shows `healthy`.

- [ ] **Step 2: Write the failing integration test**

`tests/test_integration.py`:
```python
import asyncio
import json
import os
import uuid

import aio_pika
import httpx
import pytest

from notifier.config import Settings
from notifier.main import run
from notifier.topology import DLQ_QUEUE, EXCHANGE, MAIN_QUEUE, ROUTING_KEY, declare

pytestmark = pytest.mark.integration

AMQP_URL = os.environ.get("AMQP_URL", "amqp://notifier:notifier@localhost:5672/")
DELAYS = [1, 2]


@pytest.fixture
async def channel():
    try:
        conn = await aio_pika.connect(AMQP_URL, timeout=3)
    except Exception:
        pytest.skip("RabbitMQ not reachable; run: docker compose up -d rabbitmq")
    async with conn:
        ch = await conn.channel()
        main_q, retry_names = await declare(ch, DELAYS)
        for name in [MAIN_QUEUE, DLQ_QUEUE, *retry_names]:
            await (await ch.get_queue(name)).purge()
        yield ch


@pytest.fixture
async def service():
    """Start run() with a fake Telegram. Returns (calls, set_responses)."""
    calls = []
    responses = [httpx.Response(200, json={"ok": True})]

    def respond(request):
        calls.append(json.loads(request.content))
        return responses[min(len(calls), len(responses)) - 1]

    http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    settings = Settings(amqp_url=AMQP_URL, telegram_bot_token="test-token", retry_delays=DELAYS)
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, http=http, stop=stop))

    def set_responses(*rs):
        responses[:] = rs

    yield calls, set_responses
    stop.set()
    await asyncio.wait_for(task, 10)
    await http.aclose()


async def publish(ch, text="hello"):
    ex = await ch.get_exchange(EXCHANGE)
    body = json.dumps({"id": str(uuid.uuid4()), "version": 1, "channel": "telegram",
                       "recipient": {"chat_id": 99}, "text": text}).encode()
    await ex.publish(aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                     routing_key=ROUTING_KEY)


async def wait_for(pred, timeout=15.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.1)


async def get_dlq_message(ch, timeout=15.0):
    q = await ch.get_queue(DLQ_QUEUE)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        msg = await q.get(no_ack=True, fail=False)
        if msg:
            return msg
        await asyncio.sleep(0.2)
    raise AssertionError("no message in DLQ")


async def test_delivered(channel, service):
    calls, _ = service
    await publish(channel)
    await wait_for(lambda: len(calls) == 1)
    assert calls[0]["chat_id"] == 99


async def test_5xx_then_success_is_retried(channel, service):
    calls, set_responses = service
    set_responses(httpx.Response(500, json={"ok": False}), httpx.Response(200, json={"ok": True}))
    await publish(channel)
    await wait_for(lambda: len(calls) == 2)  # second call arrives after the 1 s retry queue


async def test_400_goes_to_dlq(channel, service):
    calls, set_responses = service
    set_responses(httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"}))
    await publish(channel)
    msg = await get_dlq_message(channel)
    assert "400" in str(msg.headers["x-error"])
    assert len(calls) == 1


async def test_invalid_body_goes_to_dlq(channel, service):
    calls, _ = service
    ex = await channel.get_exchange(EXCHANGE)
    await ex.publish(aio_pika.Message(b"not json"), routing_key=ROUTING_KEY)
    msg = await get_dlq_message(channel)
    assert "invalid envelope:" in str(msg.headers["x-error"])
    assert calls == []
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_integration.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notifier.main'` (or `notifier.topology`).

- [ ] **Step 4: Implement `notifier/topology.py`**

```python
from collections.abc import Mapping
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue

EXCHANGE = "notifications"
ROUTING_KEY = "telegram.send"
MAIN_QUEUE = "notify.telegram"
DLQ_QUEUE = "notify.dlq"

# Headers the broker adds on dead-lettering/redelivery; do not copy them into new messages.
_BROKER_HEADERS = {"x-death", "x-delivery-count", "x-first-death-exchange",
                   "x-first-death-queue", "x-first-death-reason",
                   "x-last-death-exchange", "x-last-death-queue", "x-last-death-reason"}


def retry_queue_name(delay: int) -> str:
    return f"notify.retry.{delay}s"


async def declare(channel: AbstractChannel, retry_delays: list[int]) -> tuple[AbstractQueue, list[str]]:
    """Idempotent. Changing arguments of an existing queue fails with PRECONDITION_FAILED: delete the queue first."""
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    await channel.declare_queue(DLQ_QUEUE, durable=True)
    main = await channel.declare_queue(MAIN_QUEUE, durable=True, arguments={
        "x-queue-type": "quorum",
        # Poison messages (handler keeps crashing) land in the DLQ instead of looping.
        "x-delivery-limit": 20,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": DLQ_QUEUE,
    })
    await main.bind(exchange, ROUTING_KEY)

    names = []
    for delay in retry_delays:
        name = retry_queue_name(delay)
        await channel.declare_queue(name, durable=True, arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY,
        })
        names.append(name)
    return main, names


class Publisher:
    """Publishes through a channel with publisher confirms: each call returns only after the broker confirms."""

    def __init__(self, channel: AbstractChannel):
        self._channel = channel

    async def retry(self, body: bytes, headers: Mapping[str, Any], queue: str, attempt: int) -> None:
        await self._publish(body, {**_clean(headers), "x-attempt": attempt}, queue)

    async def dead_letter(self, body: bytes, headers: Mapping[str, Any], reason: str) -> None:
        await self._publish(body, {**_clean(headers), "x-error": reason[:500]}, DLQ_QUEUE)

    async def _publish(self, body: bytes, headers: dict[str, Any], queue: str) -> None:
        await self._channel.default_exchange.publish(
            aio_pika.Message(body, headers=headers, content_type="application/json",
                             delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue,
        )


def _clean(headers: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in headers.items() if k not in _BROKER_HEADERS}
```

- [ ] **Step 5: Implement `notifier/main.py`**

```python
import asyncio
import contextlib
import json
import logging
import signal

import aio_pika
import httpx

from notifier.config import Settings
from notifier.handler import Handler
from notifier.ratelimit import RateLimiter
from notifier.telegram import TelegramClient
from notifier.topology import Publisher, declare

log = logging.getLogger("notifier")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {"ts": self.formatTime(record), "level": record.levelname, "msg": record.getMessage()}
        data.update(getattr(record, "fields", {}))
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


async def run(settings: Settings, http: httpx.AsyncClient | None = None,
              stop: asyncio.Event | None = None) -> None:
    if stop is None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):  # Windows: Ctrl+C still works via KeyboardInterrupt
                loop.add_signal_handler(sig, stop.set)

    async with contextlib.AsyncExitStack() as stack:
        if http is None:
            http = await stack.enter_async_context(httpx.AsyncClient(timeout=10))
        connection = await aio_pika.connect_robust(settings.amqp_url)
        await stack.enter_async_context(connection)

        channel = await connection.channel()  # publisher confirms are on by default
        await channel.set_qos(prefetch_count=settings.prefetch)
        queue, retry_queues = await declare(channel, settings.retry_delays)

        handler = Handler(
            TelegramClient(settings.telegram_bot_token.get_secret_value(), http),
            RateLimiter(),
            Publisher(channel),
            retry_queues,
        )

        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            # Exception -> reject with requeue. Normal return -> ack (after any retry/DLQ publish was confirmed).
            async with message.process(requeue=True):
                await handler.handle(message.body, dict(message.headers or {}))

        tag = await queue.consume(on_message)
        log.info("started", extra={"fields": {"queue": queue.name, "retry_queues": retry_queues}})
        await stop.wait()
        # ponytail: in-flight messages are requeued on close and may be sent twice; drain first if that matters.
        await queue.cancel(tag)
        log.info("stopped")


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run integration tests to verify they pass**

Run: `uv run pytest tests/test_integration.py -v`
Expected: 4 PASS. If they are SKIPPED, RabbitMQ is not running: run `docker compose up -d rabbitmq` and retry.

If a test fails with `PRECONDITION_FAILED` on a queue, an older queue with different arguments exists. Delete it in the management UI (http://localhost:15672, user `notifier` / `notifier`) and rerun.

- [ ] **Step 7: Run the full suite**

Run: `uv run pytest -v`
Expected: all PASS.

---

### Task 7: Docker image and end-to-end smoke test

**Files:**
- Create: `Dockerfile`, `.dockerignore`
- Modify: `docker-compose.yml` (add `notifier` service)

**Interfaces:**
- Consumes: `python -m notifier.main` (Task 6), env vars from `Settings` (Task 1).

- [ ] **Step 1: Create `.dockerignore`**

```
.venv
__pycache__
.pytest_cache
tests
docs
```

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ENV UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY notifier ./notifier
CMD ["uv", "run", "--no-sync", "python", "-m", "notifier.main"]
```

- [ ] **Step 3: Add the `notifier` service to `docker-compose.yml`**

Add under `services:`:
```yaml
  notifier:
    build: .
    environment:
      AMQP_URL: amqp://notifier:notifier@rabbitmq:5672/
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:?set TELEGRAM_BOT_TOKEN}
      LOG_LEVEL: INFO
    depends_on:
      rabbitmq:
        condition: service_healthy
    restart: unless-stopped
```

- [ ] **Step 4: Build and start**

```bash
docker compose build notifier
TELEGRAM_BOT_TOKEN=<your bot token> docker compose up -d
docker compose logs -f notifier
```
In PowerShell set the variable first: `$env:TELEGRAM_BOT_TOKEN = "<your bot token>"`, then `docker compose up -d`.
Expected: a JSON log line `"msg": "started"` listing `notify.telegram` and the three retry queues.

- [ ] **Step 5: End-to-end smoke test with real Telegram**

1. Create a bot with @BotFather and add it to a test group. Get the group chat id (send a message in the group, then open `https://api.telegram.org/bot<token>/getUpdates` and read `message.chat.id`).
2. In the management UI (http://localhost:15672, `notifier` / `notifier`), open exchange `notifications` and publish with routing key `telegram.send`, property `delivery_mode = 2`, payload:
```json
{"id":"6f1c2c1e-6a0b-4b8e-9d2a-2f6f1f0c9a11","version":1,"channel":"telegram","recipient":{"chat_id":<group chat id>},"text":"<b>Hello</b> from notifier","parse_mode":"HTML"}
```
3. Expected: message appears in the Telegram group. Log line has `"outcome": "sent"`.
4. Publish the same payload again. Expected: no new Telegram message. Log line has `"outcome": "duplicate"`.
5. Publish with `"chat_id": 1`. Expected: `notify.dlq` holds the message with an `x-error` header starting with `400`.
6. Check that `docker compose logs notifier` does not contain the bot token.
