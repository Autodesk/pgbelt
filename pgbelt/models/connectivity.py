from __future__ import annotations


from pydantic import BaseModel
from pydantic import computed_field

from pgbelt.models.base import CommandResult


class ConnectivityCheckRow(BaseModel):
    """Connectivity results for a single database pair.

    The ``*_owner_query`` fields validate that the *owner* credentials
    (not just root) can authenticate and ``SELECT 1``. They were added
    because ``belt precheck`` is the first command that opens an
    owner-pool against the source, and on a wrong / stale owner password
    against a managed RDS the asyncpg ``create_pool`` hangs for the full
    ``connect_timeout`` (60s) before surfacing a bare ``TimeoutError``
    with no message. Probing owner here at the connectivity stage keeps
    that failure visible on the per-DB grid (and 6x faster) instead of
    poisoning the next stage.
    """

    db: str
    src_tcp: bool
    src_query: bool
    src_owner_query: bool
    src_to_dst_dblink: bool
    dst_tcp: bool
    dst_query: bool
    dst_owner_query: bool
    dst_to_src_dblink: bool

    @computed_field
    @property
    def all_ok(self) -> bool:
        return all(
            [
                self.src_tcp,
                self.src_query,
                self.src_owner_query,
                self.src_to_dst_dblink,
                self.dst_tcp,
                self.dst_query,
                self.dst_owner_query,
                self.dst_to_src_dblink,
            ]
        )

    @computed_field
    @property
    def failed_checks(self) -> list[str]:
        checks = {
            "src_tcp": self.src_tcp,
            "src_query": self.src_query,
            "src_owner_query": self.src_owner_query,
            "src_to_dst_dblink": self.src_to_dst_dblink,
            "dst_tcp": self.dst_tcp,
            "dst_query": self.dst_query,
            "dst_owner_query": self.dst_owner_query,
            "dst_to_src_dblink": self.dst_to_src_dblink,
        }
        return [name for name, passed in checks.items() if not passed]


class ConnectivityCheckResult(CommandResult):
    """JSON output for ``belt check-connectivity``."""

    command: str = "check-connectivity"
    results: list[ConnectivityCheckRow] = []

    @computed_field
    @property
    def overall_ok(self) -> bool:
        return all(r.all_ok for r in self.results)
