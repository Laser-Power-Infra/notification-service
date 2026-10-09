# Telegram Notification Service — Design

Date: 2026-10-01
Status: Draft, awaiting review

## Purpose

Other services publish notification requests to RabbitMQ. This service consumes them and delivers them to Telegram. Recipients are both ops group chats (alerts) and individual end users.

Success criteria:
- A producer publishes one JSON message and it arrives in the correct Telegram chat.
- Transient failures are retried automatically.
- Messages that cannot be delivered are never lost silently; they land in a dead-letter queue for inspection.

## Constraints and assumptions

- Language: Python 3.12, managed with `uv`.
- Volume: low, under 10 messages per second. One process is enough.
- Producers send final, ready-to-send text. The service does no templating.
- Only Telegram for now. The envelope carries a `channel` field so other channels can be added later without a format change.
- Delivery is at-least-once. Duplicates are suppressed best-effort (see Deduplication).
- v1 requires producers to supply a Telegram `chat_id`. Linking app users to chat IDs is phase 2.

## Protocols

- Producers to RabbitMQ: AMQP 0-9-1. Producers should use publisher confirms and publish messages as persistent.
- Message payload: JSON, versioned envelope (below).
- Service to Telegram: HTTPS calls to the Bot API `sendMessage` method. The service only sends, so v1 needs no webhook and no polling.

## RabbitMQ topology

```
producers --AMQP--> exchange "notifications" (topic, durable)
                        | routing key "telegram.send"
                        v
                   queue "notify.telegram" (durable, quorum)
                        | prefetch=10, manual ack
                        v
              notifier (single asyncio process)
                validate -> dedup -> rate-limit -> send -> ack
                        | on failure
            retryable  -> publish to "notify.retry.10s" / "notify.retry.60s" / "notify.retry.300s"
                          (each has x-message-ttl and dead-letters back to "notify.telegram")
            permanent or retries exhausted -> publish to "notify.dlq"
```

The service declares all exchanges, queues, and bindings at startup. Declarations are idempotent, so restarts are safe.

### Retry flow

- Each message carries an `x-attempt` header. A missing header means attempt 1.
- On a retryable failure, the service picks the retry queue that matches the attempt number: first retry 10 s, second 1 m, third 5 m.
- After the third retry fails, the message goes to `notify.dlq`.
- The service republishes the message (with an incremented `x-attempt`) using publisher confirms, and only then acks the original. A crash between these two steps causes a duplicate at worst, never a loss.
- The retry delays are configurable through `RETRY_DELAYS`. The number of retries equals the number of delays. Retry queues are named `notify.retry.<seconds>s`.
- The main queue has `x-delivery-limit: 20` and dead-letters to `notify.dlq`. A message that crashes the handler repeatedly ends up in the DLQ instead of looping forever.

### Error classification

| Outcome | Cause | Action |
|---|---|---|
| Success | HTTP 200, `ok: true` | Ack, record id in dedup cache |
| Retryable | Network error, timeout, HTTP 5xx | Next retry queue |
| Retryable | HTTP 429 | Wait `parameters.retry_after` seconds in process, then retry the send. If the wait exceeds 60 s, use the next retry queue instead. |
| Permanent | HTTP 400 (bad chat ID, bad markup), HTTP 403 (bot blocked or removed) | DLQ |
| Permanent | Body is not valid JSON or fails envelope validation | DLQ |

Messages in the DLQ keep their original body. The service adds an `x-error` header with a short reason.

### Rate limiting

Telegram allows about 30 messages per second per bot overall and about 1 message per second per chat. The service limits itself to 25 messages per second overall and 1 message per second per chat, using in-memory limiters. This is sufficient for one process.

## Message envelope (v1)

```json
{
  "id": "6f1c2c1e-6a0b-4b8e-9d2a-2f6f1f0c9a11",
  "version": 1,
  "channel": "telegram",
  "recipient": {"chat_id": -1001234567890},
  "text": "<b>Order #42</b> shipped",
  "parse_mode": "HTML",
  "disable_notification": false
}
```

| Field | Required | Rules |
|---|---|---|
| `id` | yes | UUID string. Used as the idempotency key. |
| `version` | yes | Must be `1`. |
| `channel` | yes | Must be `"telegram"`. |
| `recipient.chat_id` | yes | Integer. Group and channel IDs are negative. |
| `text` | yes | 1 to 4096 characters (Telegram limit). |
| `parse_mode` | no | `"HTML"`, `"MarkdownV2"`, or absent for plain text. |
| `disable_notification` | no | Boolean, default `false`. |

Unknown fields are rejected, so producer typos fail loudly in the DLQ instead of being ignored.

## Deduplication

v1 keeps an in-memory LRU cache of the last 10,000 successfully sent `id` values. A message whose `id` is in the cache is acked without sending. A restart clears the cache, so a redelivered message can be sent twice after a restart. This is accepted for v1.

## Components

```
notifier/
  main.py        entrypoint: load config, connect_robust, declare topology, consume, graceful shutdown on SIGTERM/SIGINT
  config.py      pydantic-settings model for environment variables
  topology.py    declare exchange, main queue, retry queues, DLQ, bindings
  envelope.py    pydantic model for the envelope
  telegram.py    send(envelope) via httpx; returns Ok, Retryable(retry_after), or Permanent(reason)
  handler.py     per-message flow: validate, dedup, rate-limit, send, then ack / retry / DLQ
  ratelimit.py   global and per-chat limiters
tests/
Dockerfile
docker-compose.yml
pyproject.toml
```

Each module has one job:
- `telegram.py` knows nothing about RabbitMQ. It is tested with `httpx.MockTransport`.
- `handler.py` decides what to do with a result. The decision function is pure, so it can be tested without a broker.
- `topology.py` is the only place that knows queue names and arguments.

Dependencies: `aio-pika`, `httpx`, `pydantic`, `pydantic-settings`. Test dependencies: `pytest`, `pytest-asyncio`. No Telegram SDK, because the service calls only one endpoint.

### Package management

All packages are managed with `uv`. Do not use `pip` or `poetry`, and do not create `requirements.txt` files.

- Dependencies are declared in `pyproject.toml` and locked in `uv.lock`. Commit both.
- Add a runtime dependency with `uv add <pkg>` and a test dependency with `uv add --dev <pkg>`.
- Install with `uv sync`. Run commands with `uv run`, for example `uv run pytest` or `uv run python -m notifier.main`.
- The Python version is pinned in `.python-version` (3.12). Install it with `uv python install`.
- The `Dockerfile` installs dependencies with `uv sync --frozen --no-dev`.

## Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `AMQP_URL` | yes | — | RabbitMQ connection URL |
| `TELEGRAM_BOT_TOKEN` | yes | — | Bot token from BotFather. Never logged. |
| `PREFETCH` | no | `10` | Max unacked messages in flight |
| `RETRY_DELAYS` | no | `10,60,300` | Retry delays in seconds, comma-separated |
| `LOG_LEVEL` | no | `INFO` | Log level |

## Observability

- JSON logs, one line per message outcome, with `id`, `chat_id`, `attempt`, `outcome`, and `error` when relevant.
- DLQ depth in the RabbitMQ management UI is the alert signal.
- A Prometheus metrics endpoint is out of scope until Prometheus exists in the environment.

## Deployment

- `Dockerfile` for the service.
- `docker-compose.yml` with `rabbitmq:management` and the notifier, for local development and the integration tests.

## Testing

Unit tests:
- Envelope validation: valid message, missing fields, unknown fields, text too long, wrong version.
- Telegram response mapping: 200, 400, 403, 429 with `retry_after`, 5xx, timeout.
- Retry decision: attempt number to retry queue, exhausted retries to DLQ, permanent errors to DLQ.
- Dedup cache: second send of the same `id` is skipped.

Integration tests (RabbitMQ from docker-compose, fake Telegram HTTP server):
- Valid message is delivered and acked.
- Fake server returns 5xx once, then 200: message is retried and delivered.
- Fake server returns 400: message lands in `notify.dlq` with an `x-error` header.

Manual check: run `docker compose up`, publish a sample envelope from the management UI, and confirm it arrives in a real Telegram test chat.

## Out of scope for v1 (phase 2)

- User linking: the app generates a `t.me/<bot>?start=<token>` link. A long-polling listener task in the same process receives `/start <token>` and stores `user_id -> chat_id` in SQLite.
- The envelope accepts `recipient.user_id` as an alternative to `chat_id`, resolved through that table.
- Deduplication persisted in the same SQLite database, so it survives restarts.
- Other channels (email, SMS, push).
