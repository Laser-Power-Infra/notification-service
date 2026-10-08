import asyncio
import json
import os
import uuid

import aio_pika
import httpx
import pytest

from notifier.config import Settings
from notifier.main import run
from notifier.topology import EXCHANGE, MAIN_QUEUE, RETRY_QUEUE, Publisher, declare

pytestmark = pytest.mark.integration

AMQP_URL = os.environ.get("AMQP_URL", "amqp://notifier:notifier@localhost:5672/")


@pytest.fixture
async def channel():
    try:
        conn = await aio_pika.connect(AMQP_URL, timeout=3)
    except Exception:
        pytest.skip("RabbitMQ not reachable; run: docker compose up -d rabbitmq")
    async with conn:
        ch = await conn.channel()
        await declare(ch)
        for name in (MAIN_QUEUE, RETRY_QUEUE):
            await (await ch.get_queue(name)).purge()
        yield ch


def fake_upstreams():
    """One MockTransport for both platforms. Returns (calls, set_responses, transport)."""
    calls = {"telegram": [], "whatsapp": []}
    responses = {"telegram": [httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})],
                 "whatsapp": [httpx.Response(201, json={"key": {"id": "WAID1"}})]}

    def respond(request):
        platform = "telegram" if request.url.host == "api.telegram.org" else "whatsapp"
        calls[platform].append(json.loads(request.content))
        rs = responses[platform]
        return rs[min(len(calls[platform]), len(rs)) - 1]

    def set_responses(platform, *rs):
        responses[platform][:] = rs

    return calls, set_responses, httpx.MockTransport(respond)


@pytest.fixture
async def worker(request):
    max_retries = getattr(request, "param", 12)
    calls, set_responses, transport = fake_upstreams()
    http = httpx.AsyncClient(transport=transport)
    settings = Settings(amqp_url=AMQP_URL, telegram_bot_token="test-token", evolution_url="http://evo:8080",
                        evolution_api_key="k", evolution_instance="test",
                        retry_interval_seconds=1, max_retries=max_retries)
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, http=http, stop=stop))
    yield calls, set_responses
    stop.set()
    await asyncio.wait_for(task, 10)
    await http.aclose()


async def publish(ch, platform="telegram", body=None):
    if body is None:
        recipient = {"chat_id": 99} if platform == "telegram" else {"to": "120363295648424210@g.us"}
        body = json.dumps({"id": str(uuid.uuid4()), "version": 1, "notification_platform": platform,
                           "recipient": recipient, "text": "hello"}).encode()
    ex = await ch.get_exchange(EXCHANGE)
    await ex.publish(aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=platform)


async def wait_for(pred, timeout=15.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.1)


async def queues_empty(ch):
    for name in (MAIN_QUEUE, RETRY_QUEUE):
        q = await ch.declare_queue(name, passive=True)
        if q.declaration_result.message_count:
            return False
    return True


async def wait_for_consumer(ch, timeout=10.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        q = await ch.declare_queue(MAIN_QUEUE, passive=True)
        if q.declaration_result.consumer_count:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("worker never started consuming")


async def test_both_platforms_delivered_by_one_worker(channel, worker):
    calls, _ = worker
    await publish(channel, "telegram")
    await publish(channel, "whatsapp")
    await wait_for(lambda: len(calls["telegram"]) == 1 and len(calls["whatsapp"]) == 1)
    assert calls["telegram"][0]["chat_id"] == 99
    assert calls["whatsapp"][0] == {"number": "120363295648424210@g.us", "text": "hello"}


async def test_5xx_then_success_is_retried(channel, worker):
    calls, set_responses = worker
    set_responses("telegram", httpx.Response(500, json={"ok": False}), httpx.Response(200, json={"ok": True}))
    await publish(channel)
    await wait_for(lambda: len(calls["telegram"]) == 2)  # second call after the 1 s retry queue


@pytest.mark.parametrize("worker", [2], indirect=True)
async def test_retries_exhausted_then_dropped(channel, worker):
    calls, set_responses = worker
    set_responses("whatsapp", httpx.Response(500))
    await publish(channel, "whatsapp")
    await wait_for(lambda: len(calls["whatsapp"]) == 3)  # first try + 2 retries
    await asyncio.sleep(2.5)  # longer than one retry interval: no further attempt
    assert len(calls["whatsapp"]) == 3
    assert await queues_empty(channel)


async def test_permanent_error_dropped(channel, worker):
    calls, set_responses = worker
    set_responses("telegram", httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"}))
    await publish(channel)
    await wait_for(lambda: len(calls["telegram"]) == 1)
    await asyncio.sleep(1.5)
    assert len(calls["telegram"]) == 1
    assert await queues_empty(channel)


async def test_invalid_body_dropped(channel, worker):
    calls, _ = worker
    await wait_for_consumer(channel)
    await publish(channel, body=b"not json")
    await asyncio.sleep(1)
    assert calls == {"telegram": [], "whatsapp": []}
    assert await queues_empty(channel)


async def test_unroutable_retry_publish_is_not_acked(channel, worker):
    # If the retry queue disappears, the retry publish is returned by the broker.
    # The original must not be acked (it is redelivered), otherwise it is silently lost.
    calls, set_responses = worker
    set_responses("telegram", httpx.Response(500, json={"ok": False}))
    await wait_for_consumer(channel)  # run() declares topology on start; delete only after that
    await (await channel.get_queue(RETRY_QUEUE)).delete(if_unused=False, if_empty=False)
    await publish(channel)
    await wait_for(lambda: len(calls["telegram"]) >= 2, timeout=10)


async def test_whatsapp_test_route_puts_message_on_queue(channel):
    from notifier.api import create_app
    from notifier.config import ApiSettings
    from notifier.envelope import parse_envelope

    app = create_app(ApiSettings(amqp_url=AMQP_URL, telegram_bot_token=None, evolution_url=None,
                                 evolution_api_key=None, evolution_instance=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/whatsapp/test", json={"to": "919876543210"})
    assert r.status_code == 202
    q = await channel.get_queue(MAIN_QUEUE)
    msg = None
    for _ in range(50):
        msg = await q.get(no_ack=True, fail=False)
        if msg:
            break
        await asyncio.sleep(0.1)
    assert msg is not None
    env = parse_envelope(msg.body)
    assert str(env.id) == r.json()["id"] and env.notification_platform == "whatsapp"


async def test_retry_interval_is_per_message(channel):
    # The interval is a per-message expiration, not a queue argument, so changing
    # RETRY_INTERVAL_SECONDS never fails with PRECONDITION_FAILED against an existing queue.
    await declare(channel)
    retry_q = await channel.declare_queue(RETRY_QUEUE, passive=True)
    assert retry_q.arguments is None or "x-message-ttl" not in (retry_q.arguments or {})
    await Publisher(channel, 1.5).retry(b"{}", {}, 2)
    msg = None
    for _ in range(20):
        msg = await retry_q.get(no_ack=True, fail=False)
        if msg:
            break
        await asyncio.sleep(0.05)
    assert msg is not None and msg.headers["x-attempt"] == 2
    assert msg.expiration == pytest.approx(1.5)
