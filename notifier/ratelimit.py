import asyncio
import time


class RateLimiter:
    """Reserve a per-recipient slot first, then a global slot, so a busy recipient does not block others."""

    def __init__(self, global_per_sec: float = 25.0, per_chat_interval: float = 1.0, *,
                 clock=time.monotonic, sleep=asyncio.sleep):
        self._global_interval = 1.0 / global_per_sec
        self._per_chat_interval = per_chat_interval
        self._next_global = 0.0
        self._next_chat: dict[str, float] = {}
        self._clock = clock
        self._sleep = sleep

    async def acquire(self, key: str) -> None:
        # Read-and-update has no await in between, so it is atomic under asyncio.
        now = self._clock()
        start = max(now, self._next_chat.get(key, now))
        self._next_chat[key] = start + self._per_chat_interval
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
