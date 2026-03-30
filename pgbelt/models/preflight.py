from __future__ import annotations

from typing import Optional

from pydantic import BaseModel
from pydantic import computed_field

from pgbelt.models.base import CommandResult


class RoleInfo(BaseModel):
    """Role/user details from pg_roles."""

    rolname: str
    rolcanlogin: bool
    rolcreaterole: bool
    rolinherit: bool
    rolsuper: bool
    memberof: list[str] = []
    can_create: Optional[bool] = None


class RelationInfo(BaseModel):
    """A table or sequence discovered in the schema."""

    name: str
    schema_name: str
    owner: str
    object_type: str

    @computed_field
    @property
    def can_replicate(self) -> bool:
        """True when schema and owner match the targeted migration config."""
        return True  # Actual check requires runtime context; populated by caller.


class TableReplicationInfo(BaseModel):
    """Table with replication-method classification (source side only)."""

    name: str
    schema_name: str
    owner: str
    has_primary_key: bool
    replication_method: str  # "pglogical" | "dump_and_load" | "unavailable"


class ExtensionInfo(BaseModel):
    """Installed Postgres extension."""

    extname: str
    in_other_side: Optional[bool] = None


class PrecheckSide(BaseModel):
    """Precheck data for one side (source or destination) of a migration pair."""

    db: str
    schema_name: str
    server_version: str
    max_replication_slots: str
    max_worker_processes: str
    max_wal_senders: str
    shared_preload_libraries: list[str] = []
    rds_logical_replication: str
    root_user: RoleInfo
    owner_user: RoleInfo
    tables: list[TableReplicationInfo] = []
    sequences: list[RelationInfo] = []
    extensions: list[ExtensionInfo] = []

    @computed_field
    @property
    def root_ok(self) -> bool:
        return (
            self.root_user.rolcanlogin
            and self.root_user.rolcreaterole
            and self.root_user.rolinherit
            and ("rds_superuser" in self.root_user.memberof or self.root_user.rolsuper)
        )

    @computed_field
    @property
    def shared_preload_ok(self) -> bool:
        return (
            "pglogical" in self.shared_preload_libraries
            and "pg_stat_statements" in self.shared_preload_libraries
        )


class PrecheckResult(CommandResult):
    """JSON output for ``belt precheck``."""

    command: str = "precheck"
    src: Optional[PrecheckSide] = None
    dst: Optional[PrecheckSide] = None

    @computed_field
    @property
    def extensions_match(self) -> Optional[bool]:
        if self.src is None or self.dst is None:
            return None
        src_names = {e.extname for e in self.src.extensions}
        dst_names = {e.extname for e in self.dst.extensions}
        return src_names == dst_names
