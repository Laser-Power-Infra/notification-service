# Single Queue, Fixed Retry, Test Routes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Collapse to queues `notifications` + `notifications.retry`, dispatch on `notification_platform`, fixed 5 s retry up to 12 times then drop, and add `POST /telegram/test` / `POST /whatsapp/test`.

**Architecture:** One worker consumes `notifications`, builds a sender and rate limiter per configured platform, and acks every message after a confirmed retry publish or a logged drop. The API gets an injectable `publish(routing_key, body)` used by the test routes.

**Tech Stack:** unchanged (Python 3.12, uv, aio-pika, httpx, pydantic, FastAPI).

**Spec:** `docs/superpowers/specs/2026-10-02-single-queue-design.md`

**Note:** The user asked to go straight from design to implementation, so this plan lists tasks, interfaces and tests without full code listings. Each task is still RED → GREEN. No git commits (user rule).

## Global Constraints

- uv only. No commits.
- Queue names exactly `notifications`, `notifications.retry`; exchange `notifications` (topic) bound `#`; retry dead-letters to exchange `notifications` with key `retry`.
- Defaults: `RETRY_INTERVAL_SECONDS=5`, `MAX_RETRIES=12`, `TELEGRAM_RATE_PER_SEC=25`, `WHATSAPP_RATE_PER_SEC=2`, `RATE_PER_RECIPIENT_INTERVAL=1.0`.
- Permanent failures and exhausted retries: ack + log `outcome: "dropped"`. No DLQ anywhere.
- Secrets never in logs, headers or API responses.

## Review Focus

1. A message whose platform has no credentials in the worker: dropped with reason, never crashes or loops. Test in Task 3.
2. `x-attempt` far above `MAX_RETRIES` (e.g. stale header from old retry queues): dropped, not retried forever. Test in Task 2.
3. Producer still sending `channel`: dropped with a readable "invalid envelope" reason. Test in Task 1.
4. Test route called while the worker never ran (exchange/queue missing): 503 with a clear hint, not 500. Test in Task 5.
5. `MAX_RETRIES=0`: first retryable failure drops immediately. Test in Task 2.

## Tasks

### Task 1: Envelope field rename
- Modify `notifier/envelope.py`: `channel` → `notification_platform` on both models and the discriminator.
- Update tests in `tests/test_envelope.py`, `tests/test_telegram.py`, `tests/test_whatsapp.py`, `tests/test_handler.py` bodies.
- New tests: `test_old_channel_field_rejected`, `test_unknown_platform_rejected`.

### Task 2: Config + decide()
- `notifier/config.py`: remove `retry_delays`, `rate_global_per_sec`; add `retry_interval_seconds: float = 5 (>0)`, `max_retries: int = 12 (>=0)`, `telegram_rate_per_sec: float = 25 (>0)`, `whatsapp_rate_per_sec: float = 2 (>0)`; `ApiSettings.amqp_url: str | None`; `Settings.amqp_url: str` required; `configured_platforms() -> list[str]`.
- `notifier/handler.py`: actions `Ack`, `Retry(attempt, reason)`, `Drop(reason)`; `decide(result, attempt, max_retries)`.
- Tests: config defaults/validation; `decide` cases incl. attempt 13 with max 12 → drop, attempt 999 → drop, max 0 → drop.

### Task 3: Handler dispatch, topology, worker entry point
- `notifier/topology.py`: `EXCHANGE`, `MAIN_QUEUE="notifications"`, `RETRY_QUEUE="notifications.retry"`, `declare(channel, retry_interval_seconds) -> AbstractQueue`, `Publisher(channel).retry(body, headers, attempt)`.
- `notifier/handler.py`: `Handler(senders: dict[str, sender], limiters: dict[str, limiter], publisher, *, max_retries=12, ...)`; invalid payload → drop; platform not in senders → drop "<platform> not configured"; Retry → `publisher.retry`; Drop → log only.
- `notifier/main.py`: `run(settings, http=None, stop=None)` builds senders/limiters for `configured_platforms()`; raises `ValueError` if none; no `--channel`.
- Tests: handler unit tests rewritten for the new constructor; `tests/test_main.py` (no platform configured → ValueError); `tests/test_integration.py` rewritten per spec Testing/Integration.

### Task 4: Compose + README
- Single `notifier` service with both platforms' env; remove `telegram-worker`, `whatsapp-worker`; `api` gets `AMQP_URL` and `depends_on` rabbitmq healthy.
- README: new queue layout, payload field, retry behavior, test routes.

### Task 5: Test routes
- `notifier/api.py`: `create_app(settings, http=None, publish=None)`; default publish opens an AMQP connection per request (`on_return_raises=True`, passive `get_exchange`), maps not-found/unroutable → 503, other errors → 502.
- Routes `POST /telegram/test`, `POST /whatsapp/test` → 202 `{id, notification_platform}`.
- Tests: unit with fake publish (body valid, routing key, default text, 422, 503 not configured, 503 unroutable, 502 broker error); integration: POST `/whatsapp/test` with real broker puts a message on `notifications`.

### Task 6: Live verification
- Rebuild stack, check both queues exist and only those are declared by the new code, send through `/telegram/test` to `-1004317452395`, check `outcome: "sent"`. WhatsApp live send after the user scans the QR.
