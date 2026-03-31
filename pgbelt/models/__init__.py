"""
Pydantic V2 models for ``belt --json`` output.

When to use ``CommandResult`` directly vs. create a subclass
------------------------------------------------------------

Use **CommandResult** directly when the command's output is just pass/fail
plus optional step tracking and a handful of extras.  Put command-specific
values in the ``detail`` dict.  Examples: setup, teardown variants, analyze,
load-constraints, revoke-logins.

Create a **subclass** of ``CommandResult`` when the command produces
per-item structured data that a consumer would iterate, filter, or
aggregate -- e.g. per-table validation results, per-index status,
per-sequence sync detail.  The subclass lives in its own file grouped
by domain (sync.py, schema.py, etc.).

Breaking changes to any model require a pgbelt version bump because
PGBaaS deserializes against the same types.
"""

from pgbelt.models.base import CommandError
from pgbelt.models.base import CommandResult
from pgbelt.models.base import StepResult
from pgbelt.models.base import StepStatus
from pgbelt.models.connectivity import ConnectivityCheckResult
from pgbelt.models.connectivity import ConnectivityCheckRow
from pgbelt.models.connections import ConnectionsResult
from pgbelt.models.connections import ConnectionsRow
from pgbelt.models.connections import ConnectionsSide
from pgbelt.models.preflight import PrecheckResult
from pgbelt.models.preflight import PrecheckSide
from pgbelt.models.schema import CreateIndexesResult
from pgbelt.models.schema import DiffSchemaRow
from pgbelt.models.schema import DiffSchemasResult
from pgbelt.models.schema import IndexDetail
from pgbelt.models.status import StatusResult
from pgbelt.models.status import StatusRow
from pgbelt.models.sync import SyncSequencesResult
from pgbelt.models.sync import SyncTablesResult
from pgbelt.models.sync import ValidateDataResult

__all__ = [
    # Base -- used directly by simple commands (setup, teardown, analyze, etc.)
    "CommandError",
    "CommandResult",
    "StepResult",
    "StepStatus",
    # Rich result models -- commands with structured per-item output
    "ConnectivityCheckResult",
    "ConnectivityCheckRow",
    "ConnectionsResult",
    "ConnectionsRow",
    "ConnectionsSide",
    "PrecheckResult",
    "PrecheckSide",
    "CreateIndexesResult",
    "DiffSchemaRow",
    "DiffSchemasResult",
    "IndexDetail",
    "StatusResult",
    "StatusRow",
    "SyncSequencesResult",
    "SyncTablesResult",
    "ValidateDataResult",
]
