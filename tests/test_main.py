import pytest

from notifier.config import Settings
from notifier.main import run


async def test_run_refuses_when_no_platform_configured():
    s = Settings(amqp_url="amqp://nowhere:1/", telegram_bot_token=None,
                 evolution_url=None, evolution_api_key=None, evolution_instance=None)
    with pytest.raises(ValueError, match="no platform configured"):
        await run(s)
