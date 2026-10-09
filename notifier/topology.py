from collections.abc import Mapping
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue

EXCHANGE = "notifications"
MAIN_QUEUE = "notifications"
RETRY_QUEUE = "notifications.retry"

# Headers the broker adds on dead-lettering/redelivery; do not copy them into new messages.
_BROKER_HEADERS = {"x-death", "x-delivery-count", "x-first-death-exchange",
                   "x-first-death-queue", "x-first-death-reason",
                   "x-last-death-exchange", "x-last-death-queue", "x-last-death-reason"}


async def declare(channel: AbstractChannel) -> AbstractQueue:
    """Idempotent. Changing arguments of an existing queue fails with PRECONDITION_FAILED: delete the queue first."""
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    main = await channel.declare_queue(MAIN_QUEUE, durable=True, arguments={
        "x-queue-type": "quorum",
        # A message that keeps crashing the handler is dropped after 20 deliveries (no DLQ by design).
        "x-delivery-limit": 20,
    })
    await main.bind(exchange, "#")
    # Not bound to the exchange: the worker publishes here directly; when a message's expiration
    # passes it goes back to main. TTL is per message, so changing the interval needs no queue change.
    await channel.declare_queue(RETRY_QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": EXCHANGE,
        "x-dead-letter-routing-key": "retry",
    })
    return main


class Publisher:
    """Publishes through a channel with publisher confirms: each call returns only after the broker confirms."""

    def __init__(self, channel: AbstractChannel, retry_interval_seconds: float):
        self._channel = channel
        self._interval = retry_interval_seconds

    async def retry(self, body: bytes, headers: Mapping[str, Any], attempt: int) -> None:
        await self._channel.default_exchange.publish(
            aio_pika.Message(body, headers={**_clean(headers), "x-attempt": attempt},
                             content_type="application/json", delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                             expiration=self._interval),
            routing_key=RETRY_QUEUE,
        )


def _clean(headers: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in headers.items() if k not in _BROKER_HEADERS}
