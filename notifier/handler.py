import asyncio
import logging
from collections import OrderedDict
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from notifier.envelope import AnyEnvelope, parse_envelope
from notifier.results import Ok, Permanent, Retryable, SendResult

log = logging.getLogger("notifier")


@dataclass(frozen=True)
class Ack:
    pass


@dataclass(frozen=True)
class Retry:
    attempt: int  # attempt number the republished copy will carry
    reason: str


@dataclass(frozen=True)
class Drop:
    reason: str


Action = Ack | Retry | Drop


def decide(result: SendResult, attempt: int, max_retries: int) -> Action:
    if isinstance(result, Ok):
        return Ack()
    if isinstance(result, Permanent):
        return Drop(result.reason)
    if attempt <= max_retries:
        return Retry(attempt + 1, result.reason)
    return Drop(f"retries exhausted: {result.reason}")


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
    """Validates, dedups, rate-limits and sends one message; returning means "ack", raising means "requeue"."""

    def __init__(self, senders: dict, limiters: dict, publisher, *, max_retries: int = 12,
                 dedup_size: int = 10_000, max_inline_wait: float = 60.0, sleep=asyncio.sleep):
        self._senders = senders
        self._limiters = limiters
        self._publisher = publisher
        self._max_retries = max_retries
        self._max_inline_wait = max_inline_wait
        self._sleep = sleep
        # ponytail: in-memory dedup, lost on restart; persist in SQLite in phase 2.
        self._sent = RecentIds(dedup_size)
        self._inflight: set[UUID] = set()

    async def handle(self, body: bytes, headers: Mapping[str, Any]) -> None:
        attempt = parse_attempt(headers)
        try:
            env = parse_envelope(body)
        except ValidationError as e:
            reason = f"invalid envelope: {_summarize(e)}"
            if b'"channel"' in body:  # most likely producer mistake after the field rename
                reason += ' (field "channel" was renamed to "notification_platform")'
            _log("dropped", None, None, None, attempt, reason)
            return

        platform, key = env.notification_platform, env.recipient_key()
        if platform not in self._senders:
            _log("dropped", env.id, platform, key, attempt, f"{platform} not configured")
            return

        if env.id in self._sent or env.id in self._inflight:
            _log("duplicate", env.id, platform, key, attempt)
            return

        self._inflight.add(env.id)
        try:
            result = await self._send(env)
            action = decide(result, attempt, self._max_retries)
            if isinstance(action, Ack):
                self._sent.add(env.id)
                _log("sent", env.id, platform, key, attempt, message_id=result.message_id)
            elif isinstance(action, Retry):
                await self._publisher.retry(body, headers, action.attempt)
                _log("retry", env.id, platform, key, attempt, action.reason)
            else:
                _log("dropped", env.id, platform, key, attempt, action.reason)
        finally:
            self._inflight.discard(env.id)

    async def _send(self, env: AnyEnvelope) -> SendResult:
        sender = self._senders[env.notification_platform]
        limiter = self._limiters[env.notification_platform]
        await limiter.acquire(env.recipient_key())
        result = await sender.send(env)
        if (isinstance(result, Retryable) and result.retry_after is not None
                and result.retry_after <= self._max_inline_wait):
            await self._sleep(result.retry_after)
            await limiter.acquire(env.recipient_key())
            result = await sender.send(env)
        return result


def _log(outcome: str, id: UUID | None, platform: str | None, recipient: str | None, attempt: int,
         error: str | None = None, *, message_id: str | None = None) -> None:
    fields = {"outcome": outcome, "id": id, "platform": platform, "recipient": recipient, "attempt": attempt}
    if error:
        fields["error"] = error
    if message_id:
        fields["message_id"] = message_id
    log.info("notification", extra={"fields": fields})
