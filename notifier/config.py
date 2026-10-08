from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_REQUIRED = {
    "telegram": ("telegram_bot_token",),
    "whatsapp": ("evolution_url", "evolution_api_key", "evolution_instance"),
}


class ApiSettings(BaseSettings):
    # hide_input_in_errors: validation errors would otherwise echo env values, including secrets.
    # env_ignore_empty: compose passes blank values for platforms that are not set up.
    model_config = SettingsConfigDict(hide_input_in_errors=True, env_ignore_empty=True)

    amqp_url: str | None = None
    telegram_bot_token: SecretStr | None = None
    evolution_url: str | None = None
    evolution_api_key: SecretStr | None = None
    evolution_instance: str | None = None
    log_level: str = "INFO"

    def missing_for(self, platform: str) -> list[str]:
        return [name.upper() for name in _REQUIRED[platform] if getattr(self, name) is None]

    def configured_platforms(self) -> list[str]:
        return [p for p in _REQUIRED if not self.missing_for(p)]


class Settings(ApiSettings):
    amqp_url: str
    prefetch: int = Field(10, ge=1)
    retry_interval_seconds: float = Field(5.0, gt=0)
    max_retries: int = Field(12, ge=0)
    telegram_rate_per_sec: float = Field(25.0, gt=0)
    whatsapp_rate_per_sec: float = Field(2.0, gt=0)
    rate_per_recipient_interval: float = Field(1.0, gt=0)
