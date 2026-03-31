from __future__ import annotations

from typing import Optional

from pydantic import BaseModel
from pydantic import computed_field

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


class DiffSchemaRow(BaseModel):
    """Schema comparison result for a single database pair."""

    db: str
    result: str  # "match" | "mismatch" | "skipped"
    diff: Optional[str] = None


class DiffSchemasResult(CommandResult):
    """JSON output for ``belt diff-schemas``."""

    command: str = "diff-schemas"
    full: bool = False
    results: list[DiffSchemaRow] = []

    @computed_field
    @property
    def all_match(self) -> bool:
        return all(r.result == "match" for r in self.results if r.result != "skipped")
