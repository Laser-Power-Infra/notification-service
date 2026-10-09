# Notification service

RabbitMQ consumers that deliver notifications to Telegram and WhatsApp (via Evolution API), plus an internal lookup API for recipient IDs.

## Run

    docker compose up -d --build --remove-orphans

| Service | URL |
|---|---|
| RabbitMQ management | http://localhost:15672 (notifier / notifier) |
| Lookup API docs | http://127.0.0.1:8000/docs |
| Evolution manager | http://127.0.0.1:8080/manager |

Secrets live in `.env` (git-ignored): `TELEGRAM_BOT_TOKEN`, `EVOLUTION_API_KEY`, `EVOLUTION_INSTANCE`. `EVOLUTION_API_KEY` is a value you choose (any long random string); Evolution starts with it as its global key. Compose refuses to start without it.

## Link WhatsApp (once)

1. Open http://127.0.0.1:8080/manager and log in with `EVOLUTION_API_KEY` from `.env`.
2. Create an instance named exactly as `EVOLUTION_INSTANCE` (default `notifier`), engine Baileys.
3. Click connect and scan the QR code with WhatsApp (Linked devices).

## Find recipient IDs

- Telegram: `GET http://127.0.0.1:8000/telegram/chats`. Message the bot first (in groups send `/start@<bot>`); only the last ~24 h are visible. If `migrated_to` is set, the group was upgraded: use that ID.
- WhatsApp groups: `GET http://127.0.0.1:8000/whatsapp/groups`, use `group_id`. Individual chats: phone number with country code, digits only.

## Publish

Exchange `notifications`, persistent messages, JSON body. The routing key is not used for routing; use the platform name so it shows in the management UI. `notification_platform` decides where the message goes.

Telegram:

    {"id": "<uuid>", "version": 1, "notification_platform": "telegram",
     "recipient": {"chat_id": -1004317452395}, "text": "<b>Hi</b>", "parse_mode": "HTML"}

WhatsApp:

    {"id": "<uuid>", "version": 1, "notification_platform": "whatsapp",
     "recipient": {"to": "120363295648424210@g.us"}, "text": "*Hi*"}

Use a new `id` per notification; repeated ids are skipped as duplicates.

## Queues and retries

There are two queues: `notifications` (all platforms) and `notifications.retry`.

- Temporary failures (network, 5xx, rate limits) are retried every `RETRY_INTERVAL_SECONDS` (5) up to `MAX_RETRIES` (12) times, about one minute. After that the message is deleted.
- Permanent failures (invalid payload, unknown platform, platform not configured, wrong chat id, bot blocked, invalid WhatsApp number) are deleted immediately.
- Every outcome is logged by the `notifier` service with the message `id`: `sent`, `retry`, `dropped` (with `error`), or `duplicate`.

## Send a test message

    curl -X POST http://127.0.0.1:8000/telegram/test -H 'content-type: application/json' -d '{"chat_id": -1004317452395}'
    curl -X POST http://127.0.0.1:8000/whatsapp/test -H 'content-type: application/json' -d '{"to": "919876543210"}'

Both accept an optional `text` (Telegram also `parse_mode`), publish through RabbitMQ and return `202` with the `id`. Follow it with `docker compose logs -f notifier`.

## Develop

    uv sync
    docker compose up -d rabbitmq
    docker compose stop notifier   # a running worker would consume the test messages
    uv run pytest
