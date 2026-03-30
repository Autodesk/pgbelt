from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from pgbelt.models.base import CommandResult


class ReplicationLag(BaseModel):
    """Replication lag values from pg_stat_replication."""

    sent_lag: str
    write_lag: str
    flush_lag: str
    replay_lag: str


class StatusRow(BaseModel):
    """Replication status for a single database pair."""

    db: str
    forward_replication: str  # "unconfigured" | "initializing" | "replicating" | "down"
    back_replication: str  # "unconfigured" | "initializing" | "replicating" | "down"
    lag: Optional[ReplicationLag] = None
    src_dataset_size: Optional[str] = None
    dst_dataset_size: Optional[str] = None
    progress: Optional[str] = None


class StatusResult(CommandResult):
    """JSON output for ``belt status``."""

    command: str = "status"
    results: list[StatusRow] = []
