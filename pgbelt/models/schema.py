from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from pgbelt.models.base import CommandResult


class IndexDetail(BaseModel):
    """Result for a single CREATE INDEX operation."""

    name: str
    status: str  # "created" | "skipped_exists" | "failed"
    duration_ms: Optional[int] = None
    error: Optional[str] = None


class CreateIndexesResult(CommandResult):
    """JSON output for ``belt create-indexes``."""

    command: str = "create-indexes"
    indexes_file: Optional[str] = None
    indexes: list[IndexDetail] = []
    analyze_ran: bool = False

    @property
    def created_count(self) -> int:
        return sum(1 for i in self.indexes if i.status == "created")

    @property
    def skipped_count(self) -> int:
        return sum(1 for i in self.indexes if i.status == "skipped_exists")

    @property
    def failed_count(self) -> int:
        return sum(1 for i in self.indexes if i.status == "failed")
