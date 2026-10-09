# Single Queue, Fixed Retry, Test Routes — Design

Date: 2026-10-02
Status: Approved in chat
Supersedes: the RabbitMQ topology, retry, DLQ and worker sections of
`2026-10-01-telegram-notification-service-design.md` and
`2026-10-01-whatsapp-channel-and-api-design.md`. Everything not mentioned here
(Telegram/Evolution senders, error classification, lookup API, secrets handling,
uv-only packaging) stays as specified there.

## Purpose

Reduce RabbitMQ to two queues, route by a field in the payload, retry on a fixed
short interval, and add HTTP routes that push a test notification through the
whole pipeline.

## Topology

```
producers --> exchange "notifications" (topic, durable)
                 | binding "#" (any routing key)
                 v
           queue "notifications" (quorum, x-delivery-limit 20)  <-----------+
                 | one worker, dispatch on notification_platform            |
                 |-- "telegram" -> Telegram Bot API                         |
                 |-- "whatsapp" -> Evolution API                            |
                 | retryable failure                                        |
                 v                                                          |
           queue "notifications.retry" (per-message expiration)            |
                 | dead-letter to exchange "notifications", key "retry" ----+
```

- Exactly two queues: `notifications` and `notifications.retry`. No DLQ.
- Producers publish to exchange `notifications`. The routing key is not used for
  routing; producers should use the platform name (`telegram` / `whatsapp`) so it
  is visible in the management UI.
- The retry queue is not bound to the exchange. The worker publishes to it
  through the default exchange with a per-message `expiration` of
  `RETRY_INTERVAL_SECONDS`, so the interval can change without redeclaring the
  queue. (Changed during implementation: a queue-level `x-message-ttl` made the
  worker crash with PRECONDITION_FAILED whenever the interval changed.)
- The old queues (`notify.telegram`, `notify.whatsapp`, `notify.retry.*`,
  `notify.whatsapp.retry.*`, `notify.dlq`, `notify.whatsapp.dlq`) are no longer
  declared. Deleting them from an existing broker is a manual, confirmed step.

## Retry and deletion

- `x-attempt` header counts attempts, starting at 1 (missing or invalid header
  means 1).
- Retryable failure at attempt `n`:
  - `n <= MAX_RETRIES` (default 12): republish to `notifications.retry` with
    `x-attempt = n + 1`, publisher-confirmed, then ack the original.
  - otherwise: ack and delete, log `outcome: "dropped"`, reason
    `retries exhausted: <last error>`.
- With defaults (`RETRY_INTERVAL_SECONDS=5`, `MAX_RETRIES=12`) a message gets one
  first attempt plus up to 12 retries 5 s apart: about one minute in total.
- Permanent failures are acked and deleted immediately, logged with
  `outcome: "dropped"` and the reason:
  - payload invalid or not JSON,
  - unknown `notification_platform`,
  - platform not configured in this worker (missing credentials),
  - Telegram 400/403, Evolution 400 (except "Connection Closed", which is
    retryable).
- Telegram 429 with `retry_after <= 60` still waits in-process and resends once.
- A message that crashes the handler 20 times is dropped by RabbitMQ
  (`x-delivery-limit`, no dead-letter exchange configured).
- If the retry publish fails or is unroutable, the original is requeued (not
  acked), as before.

## Payload

`channel` is renamed to `notification_platform`. `channel` is no longer accepted
(it is an unknown field and the message is dropped).

Telegram:
```json
{"id": "<uuid>", "version": 1, "notification_platform": "telegram",
 "recipient": {"chat_id": -1004317452395}, "text": "<b>Hi</b>", "parse_mode": "HTML"}
```

WhatsApp:
```json
{"id": "<uuid>", "version": 1, "notification_platform": "whatsapp",
 "recipient": {"to": "120363295648424210@g.us"}, "text": "*Hi*"}
```

All other field rules are unchanged.

## Worker

- One worker process (`python -m notifier.main`, compose service `notifier`)
  consumes `notifications` and sends to Telegram or WhatsApp per message.
- Senders are built for each platform whose credentials are set. At least one
  platform must be configured or the worker refuses to start.
- Each platform has its own rate limiter: Telegram `TELEGRAM_RATE_PER_SEC`
  (default 25), WhatsApp `WHATSAPP_RATE_PER_SEC` (default 2), both with
  `RATE_PER_RECIPIENT_INTERVAL` (default 1.0 s) per recipient.
- Known trade-off: throttled WhatsApp messages hold prefetch slots and can delay
  Telegram messages by a few seconds during a WhatsApp burst. Raise `PREFETCH`
  if that matters.
- Dedup by `id` is unchanged.

## Configuration changes

| Variable | Change |
|---|---|
| `RETRY_DELAYS` | Removed. |
| `RETRY_INTERVAL_SECONDS` | New, default `5`, must be > 0. |
| `MAX_RETRIES` | New, default `12`, must be >= 0. |
| `RATE_GLOBAL_PER_SEC` | Removed. |
| `TELEGRAM_RATE_PER_SEC` | New, default `25`. |
| `WHATSAPP_RATE_PER_SEC` | New, default `2`. |
| `AMQP_URL` | Now also read by the API (optional there). |

## Test routes (API)

| Route | Body | Result |
|---|---|---|
| `POST /telegram/test` | `{"chat_id": int, "text"?: str, "parse_mode"?: "HTML" \| "MarkdownV2"}` | `202 {"id": "<uuid>", "notification_platform": "telegram"}` |
| `POST /whatsapp/test` | `{"to": str, "text"?: str}` | `202 {"id": "<uuid>", "notification_platform": "whatsapp"}` |

- `text` defaults to `Test message from notifier ✅`.
- The route builds a full envelope with a new `id`, validates it with the same
  model the worker uses, and publishes it persistent and publisher-confirmed to
  exchange `notifications` with the platform name as routing key.
- The route does not wait for delivery. The `id` appears in the worker log with
  `outcome` `sent`, `retry` or `dropped`.
- Errors:
  - invalid body: `422` (FastAPI validation or envelope validation),
  - `AMQP_URL` not set: `503 {"detail": "rabbitmq not configured"}`,
  - exchange missing or message unroutable (worker never started):
    `503 {"detail": "rabbitmq: message not routed (is the notifier worker running?)"}`,
  - broker unreachable or other AMQP error: `502 {"detail": "rabbitmq: <error type>"}`.
- One AMQP connection per request (test routes, low volume).

## Deployment

- Compose services: `rabbitmq`, `notifier` (single worker, both platforms'
  credentials), `api` (now with `AMQP_URL`, starts after RabbitMQ is healthy),
  `evolution-api`, `evolution-postgres`, `evolution-redis`.
- `telegram-worker` and `whatsapp-worker` are removed.

## Testing

Unit:
- Envelope: `notification_platform` selects the model; `channel` is rejected.
- `decide`: Ok → ack; Permanent → drop; Retryable with attempt `<= MAX_RETRIES` →
  retry with attempt + 1; attempt `MAX_RETRIES + 1` → drop.
- Handler: dispatch to the right sender and limiter; unconfigured platform →
  drop without sending; invalid payload → drop, nothing published.
- Config: new variables, defaults and validation.
- API test routes with a fake publisher: published body is a valid envelope,
  default text, 422, 503 not configured, 503 unroutable, 502 broker error.

Integration (RabbitMQ):
- Telegram and WhatsApp messages on the one queue both delivered by one worker.
- 5xx then 200: delivered after one retry.
- Always 5xx with `MAX_RETRIES=2`: exactly 3 sends, then both queues empty.
- 400: one send, then both queues empty.
- Invalid payload: no send, queues empty.
- Retry queue missing: original not acked (redelivered).
- `POST /whatsapp/test` against the real broker: message lands in `notifications`.
