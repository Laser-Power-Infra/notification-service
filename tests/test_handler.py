import asyncio
import json
import uuid

import pytest

from notifier.handler import Ack, Drop, Handler, RecentIds, Retry, decide, parse_attempt
from notifier.telegram import Ok, Permanent, Retryable

def body(id=None, text="hi"):
    return json.dumps({
        "id": str(id or uuid.uuid4()), "version": 1, "notification_platform": "telegram",
        "recipient": {"chat_id": 7}, "text": text,
    }).encode()


def wa_body(to="919876543210"):
    return json.dumps({"id": str(uuid.uuid4()), "version": 1, "notification_platform": "whatsapp",
                       "recipient": {"to": to}, "text": "hi"}).encode()


class FakeSender:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = 0

    async def send(self, env):
        self.calls += 1
        await asyncio.sleep(0)  # yield so concurrent handlers interleave
        return self.results.pop(0) if self.results else Ok()


FakeTelegram = FakeSender


class FakeLimiter:
    def __init__(self):
        self.keys = []

    async def acquire(self, key):
        self.keys.append(key)


class FakePublisher:
    def __init__(self):
        self.retries = []

    async def retry(self, body, headers, attempt):
        self.retries.append(attempt)


async def no_sleep(seconds):
    no_sleep.calls.append(seconds)
no_sleep.calls = []


def make(*results, max_retries=12):
    tg, pub = FakeSender(*results), FakePublisher()
    no_sleep.calls = []
    h = Handler({"telegram": tg}, {"telegram": FakeLimiter()}, pub, max_retries=max_retries, sleep=no_sleep)
    return h, tg, pub


# --- decide ---

def test_decide_ok_acks():
    assert decide(Ok(), 1, 12) == Ack()


def test_decide_permanent_drops():
    assert decide(Permanent("400 x"), 1, 12) == Drop("400 x")


@pytest.mark.parametrize("attempt", [1, 5, 12])
def test_decide_retry_until_max(attempt):
    assert decide(Retryable("500"), attempt, 12) == Retry(attempt + 1, "500")


@pytest.mark.parametrize("attempt, max_retries", [(13, 12), (999, 12), (1, 0)])
def test_decide_retries_exhausted(attempt, max_retries):
    assert decide(Retryable("500"), attempt, max_retries) == Drop("retries exhausted: 500")


# --- parse_attempt ---

@pytest.mark.parametrize("headers, expected", [
    ({}, 1), ({"x-attempt": 3}, 3), ({"x-attempt": "2"}, 2),
    ({"x-attempt": "abc"}, 1), ({"x-attempt": 0}, 1), ({"x-attempt": -4}, 1),
    ({"x-attempt": float("inf")}, 1), ({"x-attempt": None}, 1),
])
def test_parse_attempt(headers, expected):
    assert parse_attempt(headers) == expected


# --- RecentIds ---

def test_recent_ids_evicts_oldest():
    ids = RecentIds(2)
    ids.add("a"); ids.add("b"); ids.add("c")
    assert "a" not in ids and "b" in ids and "c" in ids


# --- Handler ---

async def test_success_sends_once_and_acks():
    h, tg, pub = make(Ok())
    await h.handle(body(), {})
    assert tg.calls == 1 and pub.retries == []


async def test_duplicate_id_is_skipped():
    h, tg, pub = make()
    i = uuid.uuid4()
    await h.handle(body(i), {})
    await h.handle(body(i), {})
    assert tg.calls == 1


async def test_concurrent_duplicates_send_once():
    h, tg, pub = make()
    i = uuid.uuid4()
    await asyncio.gather(h.handle(body(i), {}), h.handle(body(i), {}))
    assert tg.calls == 1


async def test_failed_id_is_not_marked_sent():
    h, tg, pub = make(Retryable("500"), Ok())
    i = uuid.uuid4()
    await h.handle(body(i), {})
    await h.handle(body(i), {"x-attempt": 2})
    assert tg.calls == 2


async def test_retryable_publishes_retry_with_next_attempt():
    h, tg, pub = make(Retryable("500"))
    await h.handle(body(), {"x-attempt": 2})
    assert pub.retries == [3]


async def test_garbage_attempt_header_treated_as_first():
    h, tg, pub = make(Retryable("500"))
    await h.handle(body(), {"x-attempt": "garbage"})
    assert pub.retries == [2]


async def test_exhausted_retries_drop_without_publish():
    h, tg, pub = make(Retryable("500"), max_retries=12)
    await h.handle(body(), {"x-attempt": 13})
    assert tg.calls == 1 and pub.retries == []


async def test_permanent_is_dropped():
    h, tg, pub = make(Permanent("403 Forbidden"))
    await h.handle(body(), {})
    assert tg.calls == 1 and pub.retries == []


@pytest.mark.parametrize("raw", [bytes([0xff, 0xfe]), b"[]", b"{}", body(text="   ")])
async def test_invalid_body_dropped_without_sending(raw):
    h, tg, pub = make()
    await h.handle(raw, {})
    assert tg.calls == 0 and pub.retries == []


async def test_unconfigured_platform_dropped():
    h, tg, pub = make()
    await h.handle(wa_body(), {})
    assert tg.calls == 0 and pub.retries == []


async def test_dispatches_by_platform():
    tg, wa, pub = FakeSender(), FakeSender(Ok("WA1")), FakePublisher()
    tl, wl = FakeLimiter(), FakeLimiter()
    h = Handler({"telegram": tg, "whatsapp": wa}, {"telegram": tl, "whatsapp": wl}, pub, sleep=no_sleep)
    await h.handle(wa_body("+919876543210"), {})
    await h.handle(body(), {})
    assert (tg.calls, wa.calls) == (1, 1)
    assert tl.keys == ["7"] and wl.keys == ["919876543210"]


async def test_short_429_waits_inline_and_resends():
    h, tg, pub = make(Retryable("429", 5.0), Ok())
    await h.handle(body(), {})
    assert no_sleep.calls == [5.0] and tg.calls == 2 and pub.retries == []


async def test_long_429_uses_retry_queue():
    h, tg, pub = make(Retryable("429", 120.0))
    await h.handle(body(), {})
    assert no_sleep.calls == [] and tg.calls == 1 and pub.retries == [2]


async def test_publisher_failure_propagates_for_requeue():
    h, tg, pub = make(Retryable("500"))

    async def boom(*a):
        raise ConnectionError("broker gone")
    pub.retry = boom
    with pytest.raises(ConnectionError):
        await h.handle(body(), {})


async def test_old_channel_field_gets_rename_hint(caplog):
    caplog.set_level("INFO", logger="notifier")
    h, tg, pub = make()
    old = json.dumps({"id": str(uuid.uuid4()), "version": 1, "channel": "telegram",
                      "recipient": {"chat_id": 7}, "text": "hi"}).encode()
    await h.handle(old, {})
    assert tg.calls == 0
    errors = [r.fields.get("error", "") for r in caplog.records if hasattr(r, "fields")]
    assert any('field "channel" was renamed to "notification_platform"' in e for e in errors)
