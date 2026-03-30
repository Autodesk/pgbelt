from __future__ import annotations

from datetime import datetime
from datetime import timezone
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class StepStatus(str, Enum):
    ok = "ok"
    skipped = "skipped"
    failed = "failed"


class CommandError(BaseModel):
    """Structured error information from a failed command."""

    error_type: str
    message: str
    detail: Optional[str] = None


class StepResult(BaseModel):
    """Outcome of a single phase/step within a command."""

    name: str
    status: StepStatus
    message: Optional[str] = None
    duration_ms: Optional[int] = None


class CommandResult(BaseModel):
    """
    Base model for all pgbelt --json output.

    Simple commands (setup, teardown variants, analyze, load-constraints) use
    this directly -- they only need pass/fail, steps, and maybe a few extras
    in ``detail``.  Commands with rich structured output (precheck, status,
    sync-sequences, etc.) extend this with their own fields.
    """

    db: str
    dc: str
    command: str
    success: bool
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: Optional[int] = None
    error: Optional[CommandError] = None
    steps: list[StepResult] = []
    detail: dict[str, Any] = {}
