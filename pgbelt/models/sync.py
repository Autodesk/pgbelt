from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from pgbelt.models.base import CommandResult


class SequenceSyncDetail(BaseModel):
    """Result for a single sequence sync operation."""

    name: str
    source_value: Optional[int] = None
    destination_value: Optional[int] = None
    synced: bool
    method: str  # "pk_max" | "source_value" | "source_value_with_stride"
    skipped_reason: Optional[str] = None


class SyncSequencesResult(CommandResult):
    """JSON output for ``belt sync-sequences``."""

    command: str = "sync-sequences"
    schema_name: Optional[str] = None
    stride: Optional[int] = None
    pk_sequences: list[SequenceSyncDetail] = []
    non_pk_sequences: list[SequenceSyncDetail] = []

    @property
    def total_synced(self) -> int:
        return sum(1 for s in self.pk_sequences + self.non_pk_sequences if s.synced)

    @property
    def total_skipped(self) -> int:
        return sum(1 for s in self.pk_sequences + self.non_pk_sequences if not s.synced)


class TableSyncDetail(BaseModel):
    """Result for a single table dump-and-load operation."""

    name: str
    loaded: bool
    skipped_reason: Optional[str] = None
    row_count: Optional[int] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None


class SyncTablesResult(CommandResult):
    """JSON output for ``belt sync-tables``."""

    command: str = "sync-tables"
    schema_name: Optional[str] = None
    discovery_mode: str = "auto"  # "auto" (PK-less discovery) | "explicit" (--table)
    tables: list[TableSyncDetail] = []

    @property
    def tables_loaded(self) -> list[str]:
        return [t.name for t in self.tables if t.loaded]

    @property
    def tables_skipped(self) -> list[str]:
        return [t.name for t in self.tables if not t.loaded]


class TableValidationDetail(BaseModel):
    """Validation result for a single table."""

    name: str
    strategy: str  # "random_100" | "latest_100" | "no_pkey_presence"
    rows_compared: Optional[int] = None
    passed: bool
    mismatch_detail: Optional[str] = None


class ValidateDataResult(CommandResult):
    """JSON output for ``belt validate-data``."""

    command: str = "validate-data"
    schema_name: Optional[str] = None
    tables: list[TableValidationDetail] = []

    @property
    def tables_passed(self) -> list[str]:
        return [t.name for t in self.tables if t.passed]

    @property
    def tables_failed(self) -> list[str]:
        return [t.name for t in self.tables if not t.passed]
