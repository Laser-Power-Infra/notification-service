import asyncio

import aio_pika
import pytest

from notifier.config import Settings
from notifier.main import run


async def test_run_starts_without_platform_and_warns(monkeypatch, caplog):
    s = Settings(amqp_url="amqp://nowhere:1/", telegram_bot_token=None,
                 evolution_url=None, evolution_api_key=None, evolution_instance=None)

    async def no_broker(url):
        raise ConnectionError("stop here")

    monkeypatch.setattr(aio_pika, "connect_robust", no_broker)
    with pytest.raises(ConnectionError, match="stop here"):
        await run(s, stop=asyncio.Event())
    assert "no platform configured" in caplog.text
