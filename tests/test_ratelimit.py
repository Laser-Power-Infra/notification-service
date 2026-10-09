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
