import json
import uuid

import pytest
from pydantic import ValidationError

from notifier.envelope import TelegramEnvelope as Envelope, WhatsAppEnvelope, parse_envelope


def body(**overrides):
    data = {
        "id": str(uuid.uuid4()),
        "version": 1,
        "notification_platform": "telegram",
        "recipient": {"chat_id": -1001234567890},
        "text": "<b>hi</b>",
        "parse_mode": "HTML",
    }
    data.update(overrides)
    return json.dumps(data).encode()


def test_valid_envelope():
    env = Envelope.model_validate_json(body())
    assert env.recipient.chat_id == -1001234567890
    assert env.parse_mode == "HTML"
    assert env.disable_notification is False


def test_optional_fields_absent():
    data = json.loads(body())
    del data["parse_mode"]
    env = Envelope.model_validate_json(json.dumps(data))
    assert env.parse_mode is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"version": 2},
        {"notification_platform": "email"},
        {"text": ""},
        {"text": "   \n "},
        {"text": "x" * 4097},
        {"parse_mode": "Markdown"},
        {"recipient": {"chat_id": "abc"}},
        {"recipient": {"chat_id": 1, "extra": 1}},
        {"id": "not-a-uuid"},
        {"unknown_field": 1},
    ],
)
def test_invalid_envelopes(overrides):
    with pytest.raises(ValidationError):
        Envelope.model_validate_json(body(**overrides))


@pytest.mark.parametrize("raw", [b"\xff\xfe", b"[]", b"not json", b""])
def test_non_envelope_bodies(raw):
    with pytest.raises(ValidationError):
        Envelope.model_validate_json(raw)


def test_max_length_text_ok():
    assert len(Envelope.model_validate_json(body(text="x" * 4096)).text) == 4096


def wa_body(**overrides):
    data = {"id": str(uuid.uuid4()), "version": 1, "notification_platform": "whatsapp",
            "recipient": {"to": "120363295648424210@g.us"}, "text": "*hi*"}
    data.update(overrides)
    return json.dumps(data).encode()


@pytest.mark.parametrize("to, expected", [
    ("120363295648424210@g.us", "120363295648424210@g.us"),
    ("919876543210", "919876543210"),
    ("+919876543210", "919876543210"),
])
def test_whatsapp_recipient_ok(to, expected):
    env = parse_envelope(wa_body(recipient={"to": to}))
    assert isinstance(env, WhatsAppEnvelope)
    assert env.recipient.to == expected
    assert env.recipient_key() == expected


@pytest.mark.parametrize("to", [
    "1234567", "1234567890123456", "+91 98765-43210", "abc", "12345@g.com",
    "@g.us", "919876543210@s.whatsapp.net", "",
])
def test_whatsapp_recipient_rejected(to):
    with pytest.raises(ValidationError):
        parse_envelope(wa_body(recipient={"to": to}))


@pytest.mark.parametrize("overrides", [
    {"text": ""}, {"text": "  \n"}, {"text": "x" * 65537}, {"parse_mode": "HTML"},
    {"recipient": {"to": "919876543210", "x": 1}},
])
def test_whatsapp_invalid(overrides):
    with pytest.raises(ValidationError):
        parse_envelope(wa_body(**overrides))


def test_whatsapp_long_text_ok():
    assert len(parse_envelope(wa_body(text="x" * 65536)).text) == 65536


def test_parse_envelope_picks_telegram():
    env = parse_envelope(body())
    assert isinstance(env, Envelope)
    assert env.recipient_key() == "-1001234567890"


@pytest.mark.parametrize("channel", ["email", None])
def test_parse_envelope_unknown_channel(channel):
    with pytest.raises(ValidationError):
        parse_envelope(body(notification_platform=channel))


def test_old_channel_field_rejected():
    data = json.loads(body())
    data["channel"] = data.pop("notification_platform")
    with pytest.raises(ValidationError):
        parse_envelope(json.dumps(data))


def test_unknown_platform_rejected():
    with pytest.raises(ValidationError):
        parse_envelope(body(notification_platform="sms"))
