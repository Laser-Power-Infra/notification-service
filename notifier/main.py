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
from notifier.whatsapp import EvolutionClient

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


def build_senders(settings: Settings, http: httpx.AsyncClient) -> tuple[dict, dict]:
    """Sender and rate limiter for every platform whose credentials are set."""
    senders, limiters = {}, {}
    for platform in settings.configured_platforms():
        if platform == "telegram":
            senders[platform] = TelegramClient(settings.telegram_bot_token.get_secret_value(), http)
            rate = settings.telegram_rate_per_sec
        else:
            senders[platform] = EvolutionClient(settings.evolution_url, settings.evolution_api_key.get_secret_value(),
                                                settings.evolution_instance, http)
            rate = settings.whatsapp_rate_per_sec
        limiters[platform] = RateLimiter(rate, settings.rate_per_recipient_interval)
    return senders, limiters


async def run(settings: Settings, http: httpx.AsyncClient | None = None,
              stop: asyncio.Event | None = None) -> None:
    if not settings.configured_platforms():
        raise ValueError("no platform configured: set TELEGRAM_BOT_TOKEN and/or "
                         "EVOLUTION_URL, EVOLUTION_API_KEY, EVOLUTION_INSTANCE")
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

        # Publisher confirms are on by default. on_return_raises: a retry publish to a missing
        # queue raises instead of "succeeding", so the original is requeued, not acked and lost.
        channel = await connection.channel(on_return_raises=True)
        await channel.set_qos(prefetch_count=settings.prefetch)
        queue = await declare(channel)

        senders, limiters = build_senders(settings, http)
        # ponytail: one queue for all platforms; throttled WhatsApp messages can hold prefetch slots
        # and delay Telegram briefly. Raise PREFETCH or split queues if that matters.
        handler = Handler(senders, limiters, Publisher(channel, settings.retry_interval_seconds),
                          max_retries=settings.max_retries)

        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            # Exception -> reject with requeue. Normal return -> ack (after any retry publish was confirmed).
            async with message.process(requeue=True):
                await handler.handle(message.body, dict(message.headers or {}))

        tag = await queue.consume(on_message)
        log.info("started", extra={"fields": {"queue": queue.name, "platforms": sorted(senders)}})
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
