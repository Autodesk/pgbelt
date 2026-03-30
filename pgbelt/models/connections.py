from __future__ import annotations


from pydantic import BaseModel

from pgbelt.models.base import CommandResult


class ConnectionsSide(BaseModel):
    """Connection summary for one side of a database pair."""

    total_connections: int
    by_user: dict[str, int] = {}


class ConnectionsRow(BaseModel):
    """Connection info for a single database pair."""

    db: str
    source: ConnectionsSide
    destination: ConnectionsSide


class ConnectionsResult(CommandResult):
    """JSON output for ``belt connections``."""

    command: str = "connections"
    exclude_users: list[str] = []
    exclude_patterns: list[str] = []
    results: list[ConnectionsRow] = []
