from dataclasses import dataclass


@dataclass(frozen=True)
class Ok:
    message_id: str | None = None


@dataclass(frozen=True)
class Retryable:
    reason: str
    retry_after: float | None = None


@dataclass(frozen=True)
class Permanent:
    reason: str


SendResult = Ok | Retryable | Permanent
