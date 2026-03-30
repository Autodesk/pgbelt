"""
Round-trip serialization / deserialization tests for all pgbelt JSON output models.

For each model we:
  1. Build a representative instance with realistic field values.
  2. Serialize with model_dump_json().
  3. Deserialize with Model.model_validate_json().
  4. Assert equality of the two instances.
  5. Spot-check computed fields where applicable.

Commands that use plain ``CommandResult`` (setup, teardown variants, analyze,
load-constraints, revoke-logins) store any extras in the ``detail`` dict.
Only commands with genuinely rich per-item output have their own subclass.
"""

from datetime import datetime, timezone

from pgbelt.models.base import CommandError, CommandResult, StepResult, StepStatus
from pgbelt.models.connectivity import ConnectivityCheckResult, ConnectivityCheckRow
from pgbelt.models.connections import (
    ConnectionsResult,
    ConnectionsRow,
    ConnectionsSide,
)
from pgbelt.models.preflight import (
    ExtensionInfo,
    PrecheckResult,
    PrecheckSide,
    RelationInfo,
    RoleInfo,
    TableReplicationInfo,
)
from pgbelt.models.schema import CreateIndexesResult, IndexDetail
from pgbelt.models.status import ReplicationLag, StatusResult, StatusRow
from pgbelt.models.sync import (
    SequenceSyncDetail,
    SyncSequencesResult,
    SyncTablesResult,
    TableSyncDetail,
    TableValidationDetail,
    ValidateDataResult,
)

FIXED_TS = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)
BASE_KWARGS = dict(db="db1", dc="dc1", timestamp=FIXED_TS)


def _round_trip(model_cls, instance):
    """Serialize then deserialize and assert equality."""
    json_bytes = instance.model_dump_json()
    restored = model_cls.model_validate_json(json_bytes)
    assert restored == instance, f"Round-trip failed for {model_cls.__name__}"
    return restored


# ---------------------------------------------------------------------------
# CommandResult used directly (simple commands)
# ---------------------------------------------------------------------------


class TestCommandResult:
    def test_success_minimal(self):
        r = CommandResult(command="test", success=True, **BASE_KWARGS)
        _round_trip(CommandResult, r)

    def test_with_error(self):
        r = CommandResult(
            command="test",
            success=False,
            error=CommandError(
                error_type="ValueError",
                message="something broke",
                detail="stack trace here",
            ),
            **BASE_KWARGS,
        )
        restored = _round_trip(CommandResult, r)
        assert restored.error.error_type == "ValueError"

    def test_with_steps_and_detail(self):
        r = CommandResult(
            command="setup",
            success=True,
            steps=[
                StepResult(name="cleanup_pglogical", status=StepStatus.ok),
                StepResult(name="configure_src_pgl", status=StepStatus.ok),
                StepResult(name="dump_schema", status=StepStatus.ok),
                StepResult(name="configure_subscription", status=StepStatus.ok),
            ],
            detail={
                "schema_applied": True,
                "tables_replicated": ["users", "orders"],
                "subscription_name": "pg1_pg2",
            },
            **BASE_KWARGS,
        )
        restored = _round_trip(CommandResult, r)
        assert len(restored.steps) == 4
        assert restored.detail["tables_replicated"] == ["users", "orders"]

    def test_setup(self):
        """``belt setup`` uses plain CommandResult with detail."""
        r = CommandResult(
            command="setup",
            success=True,
            steps=[
                StepResult(name="cleanup_pglogical", status=StepStatus.ok),
                StepResult(name="configure_src_pgl", status=StepStatus.ok),
                StepResult(name="configure_subscription", status=StepStatus.ok),
            ],
            detail={
                "schema_applied": True,
                "tables_replicated": ["users", "orders"],
                "subscription_name": "pg1_pg2",
            },
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_setup_back_replication(self):
        r = CommandResult(
            command="setup-back-replication",
            success=True,
            steps=[
                StepResult(name="configure_replication_set", status=StepStatus.ok),
                StepResult(name="configure_subscription", status=StepStatus.ok),
            ],
            detail={
                "tables_in_replication_set": ["users", "orders"],
                "subscription_name": "pg2_pg1",
            },
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_teardown_forward_replication(self):
        r = CommandResult(
            command="teardown-forward-replication",
            success=True,
            steps=[
                StepResult(name="drop_subscription_pg1_pg2", status=StepStatus.ok),
            ],
            detail={"subscription_name": "pg1_pg2"},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_teardown_back_replication(self):
        r = CommandResult(
            command="teardown-back-replication",
            success=True,
            steps=[
                StepResult(name="drop_subscription_pg2_pg1", status=StepStatus.ok),
            ],
            detail={"subscription_name": "pg2_pg1"},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_full_teardown(self):
        r = CommandResult(
            command="teardown",
            success=True,
            steps=[
                StepResult(
                    name="drop_subscription_pg2_pg1",
                    status=StepStatus.ok,
                    duration_ms=120,
                ),
                StepResult(
                    name="drop_subscription_pg1_pg2",
                    status=StepStatus.ok,
                    duration_ms=130,
                ),
                StepResult(name="teardown_replication_set", status=StepStatus.ok),
                StepResult(name="teardown_node_pg1", status=StepStatus.ok),
                StepResult(name="teardown_node_pg2", status=StepStatus.ok),
                StepResult(name="revoke_pgl", status=StepStatus.ok),
                StepResult(name="teardown_dblink", status=StepStatus.ok),
                StepResult(name="teardown_pgl_extension", status=StepStatus.ok),
            ],
            detail={"full": True},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_teardown_with_failure_step(self):
        r = CommandResult(
            command="teardown",
            success=False,
            steps=[
                StepResult(name="drop_subscription_pg2_pg1", status=StepStatus.ok),
                StepResult(
                    name="drop_subscription_pg1_pg2",
                    status=StepStatus.failed,
                    message="connection refused",
                ),
            ],
            error=CommandError(
                error_type="ConnectionError",
                message="Could not reach destination",
            ),
            detail={"full": False},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_analyze(self):
        r = CommandResult(
            command="analyze",
            success=True,
            duration_ms=5200,
            detail={"target": "destination"},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_load_constraints(self):
        r = CommandResult(
            command="load-constraints",
            success=True,
            detail={"constraints_file": "schemas/dc1/db1/invalid_constraints.sql"},
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)

    def test_revoke_logins(self):
        r = CommandResult(
            command="revoke-logins",
            success=True,
            detail={
                "config_refreshed": True,
                "owner_revoked": True,
                "users_revoked": ["app_owner", "reporting_user"],
                "users_excluded": ["pglogical", "postgres", "rdsadmin"],
            },
            **BASE_KWARGS,
        )
        _round_trip(CommandResult, r)


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------


class TestConnectivityCheckResult:
    def _make_row(self, db="db1", all_pass=True):
        return ConnectivityCheckRow(
            db=db,
            src_tcp=all_pass,
            src_query=all_pass,
            src_to_dst_dblink=all_pass,
            dst_tcp=all_pass,
            dst_query=all_pass,
            dst_to_src_dblink=all_pass,
        )

    def test_all_pass(self):
        result = ConnectivityCheckResult(
            success=True,
            results=[self._make_row()],
            **BASE_KWARGS,
        )
        restored = _round_trip(ConnectivityCheckResult, result)
        assert restored.overall_ok is True
        assert restored.results[0].all_ok is True
        assert restored.results[0].failed_checks == []

    def test_partial_failure(self):
        row = ConnectivityCheckRow(
            db="db2",
            src_tcp=True,
            src_query=True,
            src_to_dst_dblink=False,
            dst_tcp=True,
            dst_query=False,
            dst_to_src_dblink=False,
        )
        result = ConnectivityCheckResult(
            success=False,
            results=[row],
            **BASE_KWARGS,
        )
        restored = _round_trip(ConnectivityCheckResult, result)
        assert restored.overall_ok is False
        assert set(restored.results[0].failed_checks) == {
            "src_to_dst_dblink",
            "dst_query",
            "dst_to_src_dblink",
        }


# ---------------------------------------------------------------------------
# Precheck
# ---------------------------------------------------------------------------


class TestPrecheckResult:
    def _make_side(self, db="db1"):
        return PrecheckSide(
            db=db,
            schema_name="public",
            server_version="16.2",
            max_replication_slots="10",
            max_worker_processes="10",
            max_wal_senders="10",
            shared_preload_libraries=["pglogical", "pg_stat_statements"],
            rds_logical_replication="on",
            root_user=RoleInfo(
                rolname="postgres",
                rolcanlogin=True,
                rolcreaterole=True,
                rolinherit=True,
                rolsuper=True,
                memberof=["rds_superuser"],
            ),
            owner_user=RoleInfo(
                rolname="app_owner",
                rolcanlogin=True,
                rolcreaterole=False,
                rolinherit=True,
                rolsuper=False,
                memberof=[],
                can_create=True,
            ),
            tables=[
                TableReplicationInfo(
                    name="users",
                    schema_name="public",
                    owner="app_owner",
                    has_primary_key=True,
                    replication_method="pglogical",
                ),
                TableReplicationInfo(
                    name="audit_log",
                    schema_name="public",
                    owner="app_owner",
                    has_primary_key=False,
                    replication_method="dump_and_load",
                ),
            ],
            sequences=[
                RelationInfo(
                    name="users_id_seq",
                    schema_name="public",
                    owner="app_owner",
                    object_type="sequence",
                ),
            ],
            extensions=[
                ExtensionInfo(extname="pglogical"),
                ExtensionInfo(extname="uuid-ossp"),
            ],
        )

    def test_round_trip(self):
        result = PrecheckResult(
            success=True,
            src=self._make_side(),
            dst=self._make_side(),
            **BASE_KWARGS,
        )
        restored = _round_trip(PrecheckResult, result)
        assert restored.src.root_ok is True
        assert restored.src.shared_preload_ok is True
        assert restored.extensions_match is True

    def test_extensions_mismatch(self):
        src = self._make_side()
        dst = self._make_side()
        dst.extensions = [ExtensionInfo(extname="pglogical")]
        result = PrecheckResult(
            success=True,
            src=src,
            dst=dst,
            **BASE_KWARGS,
        )
        restored = _round_trip(PrecheckResult, result)
        assert restored.extensions_match is False


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


class TestSyncSequencesResult:
    def test_round_trip(self):
        result = SyncSequencesResult(
            success=True,
            schema_name="public",
            stride=1000,
            pk_sequences=[
                SequenceSyncDetail(
                    name="users_id_seq",
                    destination_value=42,
                    synced=True,
                    method="pk_max",
                ),
            ],
            non_pk_sequences=[
                SequenceSyncDetail(
                    name="global_counter_seq",
                    source_value=100,
                    destination_value=1100,
                    synced=True,
                    method="source_value_with_stride",
                ),
                SequenceSyncDetail(
                    name="old_seq",
                    source_value=5,
                    destination_value=10,
                    synced=False,
                    method="source_value",
                    skipped_reason="destination value is ahead of source",
                ),
            ],
            **BASE_KWARGS,
        )
        restored = _round_trip(SyncSequencesResult, result)
        assert restored.total_synced == 2
        assert restored.total_skipped == 1


class TestSyncTablesResult:
    def test_round_trip(self):
        result = SyncTablesResult(
            success=True,
            schema_name="public",
            discovery_mode="auto",
            tables=[
                TableSyncDetail(
                    name="audit_log", loaded=True, row_count=15000, duration_ms=3400
                ),
                TableSyncDetail(
                    name="temp_data",
                    loaded=False,
                    skipped_reason="destination table not empty",
                ),
            ],
            **BASE_KWARGS,
        )
        restored = _round_trip(SyncTablesResult, result)
        assert restored.tables_loaded == ["audit_log"]
        assert restored.tables_skipped == ["temp_data"]


class TestValidateDataResult:
    def test_all_pass(self):
        result = ValidateDataResult(
            success=True,
            schema_name="public",
            tables=[
                TableValidationDetail(
                    name="users", strategy="random_100", rows_compared=100, passed=True
                ),
                TableValidationDetail(
                    name="users", strategy="latest_100", rows_compared=100, passed=True
                ),
                TableValidationDetail(
                    name="audit_log",
                    strategy="no_pkey_presence",
                    rows_compared=100,
                    passed=True,
                ),
            ],
            **BASE_KWARGS,
        )
        restored = _round_trip(ValidateDataResult, result)
        assert len(restored.tables_passed) == 3
        assert len(restored.tables_failed) == 0

    def test_with_mismatch(self):
        result = ValidateDataResult(
            success=False,
            schema_name="public",
            tables=[
                TableValidationDetail(
                    name="orders",
                    strategy="random_100",
                    rows_compared=100,
                    passed=False,
                    mismatch_detail="Row id=42: source amount=10.00, dest amount=9.99",
                ),
            ],
            error=CommandError(
                error_type="AssertionError",
                message="Data mismatch in orders",
            ),
            **BASE_KWARGS,
        )
        restored = _round_trip(ValidateDataResult, result)
        assert restored.tables_failed == ["orders"]


# ---------------------------------------------------------------------------
# Schema (create-indexes)
# ---------------------------------------------------------------------------


class TestCreateIndexesResult:
    def test_round_trip(self):
        result = CreateIndexesResult(
            success=True,
            indexes_file="schemas/dc1/db1/indexes.sql",
            indexes=[
                IndexDetail(name="idx_users_email", status="created", duration_ms=1200),
                IndexDetail(name="idx_orders_date", status="skipped_exists"),
                IndexDetail(
                    name="idx_broken",
                    status="failed",
                    error="relation idx_broken already exists",
                ),
            ],
            analyze_ran=True,
            **BASE_KWARGS,
        )
        restored = _round_trip(CreateIndexesResult, result)
        assert restored.created_count == 1
        assert restored.skipped_count == 1
        assert restored.failed_count == 1


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


class TestConnectionsResult:
    def test_round_trip(self):
        result = ConnectionsResult(
            success=True,
            exclude_users=["datadog"],
            exclude_patterns=["%%repuser%%"],
            results=[
                ConnectionsRow(
                    db="db1",
                    source=ConnectionsSide(
                        total_connections=5,
                        by_user={"app_owner": 3, "reporting": 2},
                    ),
                    destination=ConnectionsSide(
                        total_connections=1,
                        by_user={"app_owner": 1},
                    ),
                ),
            ],
            **BASE_KWARGS,
        )
        _round_trip(ConnectionsResult, result)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestStatusResult:
    def test_replicating(self):
        result = StatusResult(
            success=True,
            results=[
                StatusRow(
                    db="db1",
                    forward_replication="replicating",
                    back_replication="replicating",
                    lag=ReplicationLag(
                        sent_lag="0", write_lag="0", flush_lag="0", replay_lag="0"
                    ),
                    src_dataset_size="n/a",
                    dst_dataset_size="n/a",
                    progress="n/a",
                ),
            ],
            **BASE_KWARGS,
        )
        _round_trip(StatusResult, result)

    def test_initializing(self):
        result = StatusResult(
            success=True,
            results=[
                StatusRow(
                    db="db1",
                    forward_replication="initializing",
                    back_replication="unconfigured",
                    lag=ReplicationLag(
                        sent_lag="unknown",
                        write_lag="unknown",
                        flush_lag="unknown",
                        replay_lag="unknown",
                    ),
                    src_dataset_size="1.2 GB",
                    dst_dataset_size="400 MB",
                    progress="33.3%",
                ),
            ],
            **BASE_KWARGS,
        )
        _round_trip(StatusResult, result)

    def test_empty_results(self):
        result = StatusResult(success=True, **BASE_KWARGS)
        restored = _round_trip(StatusResult, result)
        assert restored.results == []
