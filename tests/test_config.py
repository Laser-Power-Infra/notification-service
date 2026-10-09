import pytest
from pydantic import ValidationError

from notifier.config import ApiSettings, Settings

ALL_VARS = ("AMQP_URL", "TELEGRAM_BOT_TOKEN", "EVOLUTION_URL", "EVOLUTION_API_KEY", "EVOLUTION_INSTANCE",
            "RETRY_INTERVAL_SECONDS", "MAX_RETRIES", "TELEGRAM_RATE_PER_SEC", "WHATSAPP_RATE_PER_SEC")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in ALL_VARS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AMQP_URL", "amqp://u:p@localhost/")


def test_defaults(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:secret")
    s = Settings()
    assert s.prefetch == 10
    assert s.retry_interval_seconds == 5 and s.max_retries == 12
    assert s.telegram_rate_per_sec == 25 and s.whatsapp_rate_per_sec == 2
    assert s.rate_per_recipient_interval == 1.0
    assert s.log_level == "INFO"
    assert s.telegram_bot_token.get_secret_value() == "123:secret"


def test_retry_settings_from_env(monkeypatch):
    monkeypatch.setenv("RETRY_INTERVAL_SECONDS", "1.5")
    monkeypatch.setenv("MAX_RETRIES", "0")
    s = Settings()
    assert s.retry_interval_seconds == 1.5 and s.max_retries == 0


@pytest.mark.parametrize("var, bad", [
    ("RETRY_INTERVAL_SECONDS", "0"), ("RETRY_INTERVAL_SECONDS", "-1"), ("MAX_RETRIES", "-1"),
    ("MAX_RETRIES", "x"), ("TELEGRAM_RATE_PER_SEC", "0"), ("WHATSAPP_RATE_PER_SEC", "-2"),
])
def test_rejects_bad_values(monkeypatch, var, bad):
    monkeypatch.setenv(var, bad)
    with pytest.raises(ValidationError):
        Settings()


def test_token_hidden_in_repr(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:secret")
    assert "123:secret" not in repr(Settings())


def test_empty_values_count_as_unset(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("EVOLUTION_URL", "")
    monkeypatch.setenv("MAX_RETRIES", "")
    s = Settings()
    assert s.telegram_bot_token is None and s.evolution_url is None
    assert s.max_retries == 12
    assert s.missing_for("telegram") == ["TELEGRAM_BOT_TOKEN"]


def test_token_not_in_validation_error(monkeypatch):
    monkeypatch.delenv("AMQP_URL")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:abcdefghijklmnopqrstuvwxyz")
    with pytest.raises(ValidationError) as exc:
        Settings()
    assert "abcdefghijklmnopqrstuvwxyz"[-10:] not in str(exc.value)


def test_missing_for_whatsapp(monkeypatch):
    monkeypatch.setenv("EVOLUTION_URL", "http://evo:8080")
    s = Settings()
    assert s.missing_for("whatsapp") == ["EVOLUTION_API_KEY", "EVOLUTION_INSTANCE"]
    assert s.missing_for("telegram") == ["TELEGRAM_BOT_TOKEN"]


@pytest.mark.parametrize("env, expected", [
    ({}, []),
    ({"TELEGRAM_BOT_TOKEN": "t"}, ["telegram"]),
    ({"EVOLUTION_URL": "u", "EVOLUTION_API_KEY": "k", "EVOLUTION_INSTANCE": "i"}, ["whatsapp"]),
    ({"TELEGRAM_BOT_TOKEN": "t", "EVOLUTION_URL": "u", "EVOLUTION_API_KEY": "k", "EVOLUTION_INSTANCE": "i"},
     ["telegram", "whatsapp"]),
])
def test_configured_platforms(monkeypatch, env, expected):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert Settings().configured_platforms() == expected


def test_api_settings_need_no_amqp(monkeypatch):
    monkeypatch.delenv("AMQP_URL")
    monkeypatch.setenv("EVOLUTION_API_KEY", "evo-secret-123")
    s = ApiSettings()
    assert s.amqp_url is None
    assert s.evolution_api_key.get_secret_value() == "evo-secret-123"
    assert "evo-secret-123" not in repr(s)
