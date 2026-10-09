import re
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictInt, TypeAdapter, field_validator


def _not_blank(v: str) -> str:
    # Both APIs reject whitespace-only text; fail early with a clear reason.
    if not v.strip():
        raise ValueError("text must not be blank")
    return v


_WA_TO = re.compile(r"\+?\d{8,15}|\d+@g\.us")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TelegramRecipient(_Strict):
    chat_id: StrictInt


class TelegramEnvelope(_Strict):
    id: UUID
    version: Literal[1]
    notification_platform: Literal["telegram"]
    recipient: TelegramRecipient
    text: Annotated[str, Field(min_length=1, max_length=4096), AfterValidator(_not_blank)]
    parse_mode: Literal["HTML", "MarkdownV2"] | None = None
    disable_notification: bool = False

    def recipient_key(self) -> str:
        return str(self.recipient.chat_id)


class WhatsAppRecipient(_Strict):
    to: str

    @field_validator("to")
    @classmethod
    def _check_to(cls, v: str) -> str:
        if not _WA_TO.fullmatch(v):
            raise ValueError("to must be a group id like 1203...@g.us or a phone number with country code, digits only")
        return v.removeprefix("+")


class WhatsAppEnvelope(_Strict):
    id: UUID
    version: Literal[1]
    notification_platform: Literal["whatsapp"]
    recipient: WhatsAppRecipient
    text: Annotated[str, Field(min_length=1, max_length=65536), AfterValidator(_not_blank)]

    def recipient_key(self) -> str:
        return self.recipient.to


AnyEnvelope = TelegramEnvelope | WhatsAppEnvelope
_adapter = TypeAdapter(Annotated[AnyEnvelope, Field(discriminator="notification_platform")])


def parse_envelope(body: bytes | str) -> AnyEnvelope:
    return _adapter.validate_json(body)
