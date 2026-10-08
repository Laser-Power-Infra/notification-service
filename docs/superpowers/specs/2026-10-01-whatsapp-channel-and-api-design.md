# WhatsApp Channel and Lookup API — Design

Date: 2026-10-01
Status: Draft, awaiting review
Builds on: `2026-10-01-telegram-notification-service-design.md`

## Purpose

1. Deliver notifications to WhatsApp through Evolution API, the same way the existing service delivers to Telegram: producers publish to RabbitMQ, a worker sends, with retries and a dead-letter queue.
2. Provide internal HTTP routes that return the IDs needed to address messages: Telegram chat IDs and WhatsApp group IDs.

Success criteria:
- A producer publishes a WhatsApp envelope and the text arrives in the target WhatsApp group or personal chat.
- `GET /whatsapp/groups` lists every group the connected WhatsApp account is in, with the ID to put in `recipient.to`.
- `GET /telegram/chats` lists recent Telegram chats with their `chat_id`, including the new ID when a group was upgraded to a supergroup.
- The existing Telegram flow keeps working with no change to queue names or envelope format.

## Constraints and assumptions

- Python 3.12, packages managed only with `uv` (no `pip`, no `poetry`). Same rule as the Telegram spec.
- All HTTP routes use FastAPI, served by uvicorn.
- The API is internal only and has no authentication. In docker-compose its port is bound to `127.0.0.1`.
- Evolution API runs in this repo's docker-compose, with its own Postgres and Redis. WhatsApp is linked once by scanning a QR code.
- Recipients are WhatsApp groups and individual phone numbers.
- Producers send final text. No templating.
- Low volume. WhatsApp through Evolution (Baileys) is unofficial, so sending is deliberately slow to reduce ban risk.

## Architecture

One codebase, one Docker image, three application containers:

```
producers --AMQP--> exchange "notifications" (topic)
                       |-- "telegram.send" --> notify.telegram --> telegram-worker --> Telegram Bot API
                       |-- "whatsapp.send" --> notify.whatsapp --> whatsapp-worker --> Evolution API --> WhatsApp

internal clients --HTTP--> api (FastAPI) --> Telegram getUpdates
                                         --> Evolution fetchAllGroups
```

- `telegram-worker`: the existing notifier, unchanged in behavior.
- `whatsapp-worker`: same code path, configured for the WhatsApp channel.
- `api`: FastAPI app. Stateless; every request calls the upstream API.

A WhatsApp outage, a stuck WhatsApp queue, or an API crash does not affect Telegram delivery.

## RabbitMQ topology

All queues hang off the existing `notifications` topic exchange.

| | Telegram (unchanged) | WhatsApp (new) |
|---|---|---|
| Routing key | `telegram.send` | `whatsapp.send` |
| Main queue (quorum, `x-delivery-limit: 20`) | `notify.telegram` | `notify.whatsapp` |
| Retry queues | `notify.retry.<s>s` | `notify.whatsapp.retry.<s>s` |
| DLQ | `notify.dlq` | `notify.whatsapp.dlq` |

Telegram names are kept as they are, so existing deployments need no migration. Retry and DLQ behavior is identical to the Telegram spec: retry delays from `RETRY_DELAYS` (default `10,60,300`), `x-attempt` header, republish with publisher confirms and `on_return_raises`, then ack.

## Envelopes

The envelope model becomes a union selected by `channel`. The Telegram envelope is unchanged.

### WhatsApp envelope (v1)

```json
{
  "id": "6f1c2c1e-6a0b-4b8e-9d2a-2f6f1f0c9a11",
  "version": 1,
  "channel": "whatsapp",
  "recipient": {"to": "120363295648424210@g.us"},
  "text": "*Order #42* shipped"
}
```

| Field | Required | Rules |
|---|---|---|
| `id` | yes | UUID. Idempotency key. |
| `version` | yes | `1` |
| `channel` | yes | `"whatsapp"` |
| `recipient.to` | yes | A group ID matching `^\d+@g\.us$`, or a phone number with country code: optional leading `+`, then 8 to 15 digits. A leading `+` is removed before sending. |
| `text` | yes | 1 to 65,536 characters, not whitespace-only. WhatsApp formatting (`*bold*`, `_italic_`, `~strike~`) is passed through as-is. |

Unknown fields are rejected.

A worker only accepts envelopes for its own channel. A Telegram envelope that reaches `notify.whatsapp` (or the reverse) goes to that worker's DLQ with `x-error` naming the channel mismatch.

## WhatsApp sender

`POST {EVOLUTION_URL}/message/sendText/{EVOLUTION_INSTANCE}` with header `apikey: {EVOLUTION_API_KEY}` and body `{"number": <recipient.to without "+">, "text": <text>}`.

| Evolution reply | Outcome | Notes |
|---|---|---|
| 200 or 201 with `key.id` | Success | Log the WhatsApp message ID (`key.id`) with the envelope `id`. |
| 400 | Permanent, to DLQ | Invalid or nonexistent number, or not allowed to post in the group. `x-error` carries a short reason from the response. |
| 401, 403 | Retryable | Bad API key. Retrying lets a config fix still deliver. |
| 404 | Retryable | Instance not found. Same reasoning. |
| 5xx, timeout, network error | Retryable | |
| WhatsApp session disconnected | Retryable | Verify the exact status Evolution returns against a live instance during implementation, and map it to retryable. |

The API key never appears in logs or `x-error`. Error reasons use status code plus a short message only.

Rate limits for the WhatsApp worker: 2 messages per second overall and 1 message per second per recipient. Both are configurable. The Telegram worker keeps 25 per second overall and 1 per second per chat.

## Worker code changes

- `Handler` takes a sender object (`async send(envelope) -> Ok | Retryable | Permanent`) instead of a Telegram client, plus the channel it serves. The rate limiter keys on a recipient string so Telegram chat IDs and WhatsApp numbers share one limiter type.
- `topology.declare` and `Publisher` take a channel and derive queue names from the table above.
- Entry point: `python -m notifier.main --channel telegram` or `--channel whatsapp`. The default stays `telegram`, so the current command keeps working.

## Lookup API (FastAPI)

Served by `uvicorn notifier.api:app` on port 8000. Interactive docs at `/docs`.

### `GET /telegram/chats`

Calls Telegram `getUpdates` and returns the distinct chats seen.

```json
[
  {"chat_id": -1004317452395, "type": "supergroup", "title": "Bidyut and notifier-bot", "migrated_to": null},
  {"chat_id": -5420349998, "type": "group", "title": "Bidyut and notifier-bot", "migrated_to": -1004317452395}
]
```

- `title` is the group title, or the person's name for private chats.
- `migrated_to` is set when an update says the group was upgraded (`migrate_to_chat_id`). Producers must use the new ID.
- Limitation: `getUpdates` only holds updates from about the last 24 hours, so a chat appears only if someone messaged the bot recently. In groups the bot only sees commands such as `/start@<bot>`. This route does not pass an `offset`, so it does not consume updates.

### `GET /whatsapp/groups`

Calls `GET {EVOLUTION_URL}/group/fetchAllGroups/{EVOLUTION_INSTANCE}?getParticipants=false`.

```json
[
  {"group_id": "120363295648424210@g.us", "name": "Ops alerts", "size": 12, "announce": false}
]
```

- `announce: true` means only admins can post. Sending there fails unless the connected account is an admin.
- Individual numbers are not listed. Use the phone number with country code directly.

### `GET /health`

Returns `{"status": "ok"}`. Does not call upstream services.

### Errors

If Telegram or Evolution fails (network error, non-2xx, invalid body), the route returns `502` with `{"detail": "<service>: <status or error type>"}`. Tokens and API keys never appear in responses or logs.

## Configuration

Settings are split so each container requires only what it uses.

| Variable | Used by | Required | Default |
|---|---|---|---|
| `AMQP_URL` | workers | yes | — |
| `TELEGRAM_BOT_TOKEN` | telegram-worker, api | yes for those | — |
| `EVOLUTION_URL` | whatsapp-worker, api | yes for those | — |
| `EVOLUTION_API_KEY` | whatsapp-worker, api | yes for those | — |
| `EVOLUTION_INSTANCE` | whatsapp-worker, api | yes for those | — |
| `PREFETCH` | workers | no | `10` |
| `RETRY_DELAYS` | workers | no | `10,60,300` |
| `RATE_GLOBAL_PER_SEC` | workers | no | `25` Telegram, `2` WhatsApp |
| `RATE_PER_RECIPIENT_INTERVAL` | workers | no | `1.0` seconds |
| `LOG_LEVEL` | all | no | `INFO` |

If the API starts without one channel's settings, that channel's route returns `503` with `{"detail": "<channel> not configured"}`. The other route keeps working.

## Deployment (docker-compose)

| Service | Image | Notes |
|---|---|---|
| `rabbitmq` | `rabbitmq:4-management` | Existing. |
| `telegram-worker` | this repo | The current `notifier` service, renamed. |
| `whatsapp-worker` | this repo | `--channel whatsapp`. |
| `api` | this repo | `uvicorn notifier.api:app --host 0.0.0.0 --port 8000`, published as `127.0.0.1:8000`. |
| `evolution-api` | `evoapicloud/evolution-api` | Published as `127.0.0.1:8080`. Manager UI at `/manager`. |
| `evolution-postgres` | `postgres` | Named volume. |
| `evolution-redis` | `redis` | Named volume. |

Secrets (`TELEGRAM_BOT_TOKEN`, `EVOLUTION_API_KEY`) come from `.env`, which is git-ignored. A `README.md` documents the one-time WhatsApp setup: open the manager UI, create an instance with the name in `EVOLUTION_INSTANCE`, scan the QR code with WhatsApp.

New dependencies: `uv add fastapi uvicorn`.

## Testing

Unit:
- WhatsApp envelope validation: group ID, phone number, leading `+`, too short, letters, wrong suffix, blank text, unknown fields.
- Channel union: Telegram envelopes still validate exactly as before. Channel mismatch in a worker goes to DLQ.
- Evolution reply mapping with `httpx.MockTransport`: 201 with `key.id`, 400, 401, 404, 500, non-JSON body, network error, API key not in any reason or log.
- API routes with FastAPI's test client and mocked upstream calls: Telegram dedup by chat, `migrated_to` populated, private chat title from names; WhatsApp group list mapping; `502` on upstream failure; `503` when a channel is not configured; secrets not in responses.

Integration (RabbitMQ from docker-compose, fake Evolution via `httpx.MockTransport`):
- WhatsApp message delivered and acked.
- Evolution `400` sends the message to `notify.whatsapp.dlq` with `x-error`.
- All existing Telegram tests pass unchanged.

Manual:
- Link WhatsApp by QR code, call `GET /whatsapp/groups`, publish to one group and to one personal number, and confirm both arrive.
- Call `GET /telegram/chats` and confirm it shows the upgraded supergroup ID.

## Out of scope

- Authentication on the API.
- Sending media, buttons, mentions, or replies on WhatsApp.
- Delivery and read receipts from WhatsApp (Evolution webhooks).
- Listing individual WhatsApp contacts.
- Telegram user linking (phase 2 of the Telegram spec).
