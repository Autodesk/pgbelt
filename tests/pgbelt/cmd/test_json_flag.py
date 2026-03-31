"""
Tests for the --json flag plumbing in helpers.py.

These test the _build_json_output helper and the json_mode path in
run_with_configs without needing real database connections.
"""

import json

from pgbelt.cmd.helpers import _build_json_output
from pgbelt.models.base import CommandResult
from pgbelt.models.connectivity import ConnectivityCheckResult
from pgbelt.models.connections import ConnectionsResult
from pgbelt.models.preflight import PrecheckResult
from pgbelt.models.status import StatusResult


class TestBuildJsonOutput:
    """Tests for the generic _build_json_output helper."""

    def test_success_with_no_results(self):
        output = _build_json_output(
            command_name="setup",
            dc="dc1",
            db="db1",
            results=[None],
            success=True,
            duration_ms=100,
        )
        parsed = json.loads(output)
        assert parsed["command"] == "setup"
        assert parsed["dc"] == "dc1"
        assert parsed["db"] == "db1"
        assert parsed["success"] is True
        assert parsed["duration_ms"] == 100
        assert parsed["error"] is None

    def test_error_output(self):
        output = _build_json_output(
            command_name="setup",
            dc="dc1",
            db="db1",
            results=[],
            success=False,
            duration_ms=50,
            error=ValueError("No tables found"),
        )
        parsed = json.loads(output)
        assert parsed["success"] is False
        assert parsed["error"]["error_type"] == "ValueError"
        assert parsed["error"]["message"] == "No tables found"

    def test_db_falls_back_to_dc_when_none(self):
        output = _build_json_output(
            command_name="analyze",
            dc="dc1",
            db=None,
            results=[],
            success=True,
            duration_ms=10,
        )
        parsed = json.loads(output)
        assert parsed["db"] == "dc1"

    def test_output_is_valid_command_result(self):
        output = _build_json_output(
            command_name="teardown",
            dc="dc1",
            db="db1",
            results=[None],
            success=True,
            duration_ms=500,
        )
        result = CommandResult.model_validate_json(output)
        assert result.command == "teardown"
        assert result.success is True

    def test_simple_command_with_single_dict(self):
        output = _build_json_output(
            command_name="diff-schemas",
            dc="dc1",
            db="db1",
            results=[{"db": "db1", "result": "match"}],
            success=True,
            duration_ms=200,
        )
        parsed = json.loads(output)
        assert parsed["success"] is True
        assert parsed["detail"]["result"] == "match"

    def test_simple_command_with_multiple_dicts(self):
        output = _build_json_output(
            command_name="diff-schemas",
            dc="dc1",
            db=None,
            results=[
                {"db": "db1", "result": "match"},
                {"db": "db2", "result": "mismatch"},
            ],
            success=True,
            duration_ms=300,
        )
        parsed = json.loads(output)
        assert parsed["success"] is True
        assert len(parsed["detail"]["databases"]) == 2


class TestBuildJsonOutputRichModels:
    """Tests for commands that produce rich Pydantic models via _build_json_output."""

    def test_check_connectivity_all_pass(self):
        output = _build_json_output(
            command_name="check-connectivity",
            dc="dc1",
            db="db1",
            results=[
                {
                    "db": "db1",
                    "src_tcp": True,
                    "src_query": True,
                    "src_to_dst_dblink": True,
                    "dst_tcp": True,
                    "dst_query": True,
                    "dst_to_src_dblink": True,
                }
            ],
            success=True,
            duration_ms=150,
        )
        result = ConnectivityCheckResult.model_validate_json(output)
        assert result.command == "check-connectivity"
        assert result.overall_ok is True
        assert len(result.results) == 1
        assert result.results[0].all_ok is True
        assert result.results[0].failed_checks == []

    def test_check_connectivity_partial_failure(self):
        output = _build_json_output(
            command_name="check-connectivity",
            dc="dc1",
            db="db1",
            results=[
                {
                    "db": "db1",
                    "src_tcp": True,
                    "src_query": True,
                    "src_to_dst_dblink": False,
                    "dst_tcp": True,
                    "dst_query": False,
                    "dst_to_src_dblink": False,
                }
            ],
            success=False,
            duration_ms=150,
        )
        result = ConnectivityCheckResult.model_validate_json(output)
        assert result.success is False
        assert result.overall_ok is False
        assert set(result.results[0].failed_checks) == {
            "src_to_dst_dblink",
            "dst_query",
            "dst_to_src_dblink",
        }

    def test_check_connectivity_multi_db(self):
        output = _build_json_output(
            command_name="check-connectivity",
            dc="dc1",
            db=None,
            results=[
                {
                    "db": "db1",
                    "src_tcp": True,
                    "src_query": True,
                    "src_to_dst_dblink": True,
                    "dst_tcp": True,
                    "dst_query": True,
                    "dst_to_src_dblink": True,
                },
                {
                    "db": "db2",
                    "src_tcp": True,
                    "src_query": True,
                    "src_to_dst_dblink": True,
                    "dst_tcp": True,
                    "dst_query": True,
                    "dst_to_src_dblink": True,
                },
            ],
            success=True,
            duration_ms=200,
        )
        result = ConnectivityCheckResult.model_validate_json(output)
        assert len(result.results) == 2
        assert result.overall_ok is True

    def test_connections(self):
        output = _build_json_output(
            command_name="connections",
            dc="dc1",
            db="db1",
            results=[
                {
                    "db": "db1",
                    "src_count": 5,
                    "src_usernames": {"app_owner": 3, "reporting": 2},
                    "dst_count": 1,
                    "dst_usernames": {"app_owner": 1},
                }
            ],
            success=True,
            duration_ms=100,
        )
        result = ConnectionsResult.model_validate_json(output)
        assert result.command == "connections"
        assert len(result.results) == 1
        assert result.results[0].source.total_connections == 5
        assert result.results[0].source.by_user == {"app_owner": 3, "reporting": 2}
        assert result.results[0].destination.total_connections == 1

    def test_connections_multi_db(self):
        output = _build_json_output(
            command_name="connections",
            dc="dc1",
            db=None,
            results=[
                {
                    "db": "db1",
                    "src_count": 5,
                    "src_usernames": {"app_owner": 3, "reporting": 2},
                    "dst_count": 1,
                    "dst_usernames": {"app_owner": 1},
                },
                {
                    "db": "db2",
                    "src_count": 0,
                    "src_usernames": {},
                    "dst_count": 0,
                    "dst_usernames": {},
                },
            ],
            success=True,
            duration_ms=200,
        )
        result = ConnectionsResult.model_validate_json(output)
        assert len(result.results) == 2
        assert result.results[1].source.total_connections == 0

    def test_status_replicating(self):
        output = _build_json_output(
            command_name="status",
            dc="dc1",
            db="db1",
            results=[
                {
                    "db": "db1",
                    "pg1_pg2": "replicating",
                    "pg2_pg1": "replicating",
                    "sent_lag": "0",
                    "write_lag": "0",
                    "flush_lag": "0",
                    "replay_lag": "0",
                    "src_dataset_size": "n/a",
                    "dst_dataset_size": "n/a",
                    "progress": "n/a",
                }
            ],
            success=True,
            duration_ms=200,
        )
        result = StatusResult.model_validate_json(output)
        assert result.command == "status"
        assert len(result.results) == 1
        row = result.results[0]
        assert row.forward_replication == "replicating"
        assert row.back_replication == "replicating"
        assert row.lag.sent_lag == "0"
        assert row.src_dataset_size == "n/a"

    def test_status_multi_db(self):
        output = _build_json_output(
            command_name="status",
            dc="dc1",
            db=None,
            results=[
                {
                    "db": "db1",
                    "pg1_pg2": "replicating",
                    "pg2_pg1": "unconfigured",
                    "sent_lag": "0",
                    "write_lag": "0",
                    "flush_lag": "0",
                    "replay_lag": "0",
                },
                {
                    "db": "db2",
                    "pg1_pg2": "initializing",
                    "pg2_pg1": "unconfigured",
                    "sent_lag": "unknown",
                    "write_lag": "unknown",
                    "flush_lag": "unknown",
                    "replay_lag": "unknown",
                    "src_dataset_size": "1.2 GB",
                    "dst_dataset_size": "400 MB",
                    "progress": "33.3%",
                },
            ],
            success=True,
            duration_ms=300,
        )
        result = StatusResult.model_validate_json(output)
        assert len(result.results) == 2
        assert result.results[0].forward_replication == "replicating"
        assert result.results[1].forward_replication == "initializing"
        assert result.results[1].progress == "33.3%"

    def test_precheck_single_db(self):
        output = _build_json_output(
            command_name="precheck",
            dc="dc1",
            db="db1",
            results=[
                {
                    "src": {
                        "db": "db1",
                        "server_version": "16.2",
                        "max_replication_slots": "10",
                        "max_worker_processes": "10",
                        "max_wal_senders": "10",
                        "shared_preload_libraries": [
                            "pglogical",
                            "pg_stat_statements",
                        ],
                        "rds.logical_replication": "on",
                        "schema": "public",
                        "users": {
                            "root": {
                                "rolname": "postgres",
                                "rolcanlogin": True,
                                "rolcreaterole": True,
                                "rolinherit": True,
                                "rolsuper": True,
                                "memberof": ["rds_superuser"],
                            },
                            "owner": {
                                "rolname": "app_owner",
                                "rolcanlogin": True,
                                "rolcreaterole": False,
                                "rolinherit": True,
                                "rolsuper": False,
                                "memberof": [],
                                "can_create": True,
                            },
                        },
                        "tables": [
                            {
                                "Name": "users",
                                "Schema": "public",
                                "Owner": "app_owner",
                            }
                        ],
                        "pkeys": ["users"],
                        "sequences": [
                            {
                                "Name": "users_id_seq",
                                "Schema": "public",
                                "Owner": "app_owner",
                            }
                        ],
                        "extensions": [
                            {"extname": "pglogical"},
                            {"extname": "uuid-ossp"},
                        ],
                    },
                    "dst": {
                        "db": "db1",
                        "server_version": "16.2",
                        "max_replication_slots": "10",
                        "max_worker_processes": "10",
                        "max_wal_senders": "10",
                        "shared_preload_libraries": [
                            "pglogical",
                            "pg_stat_statements",
                        ],
                        "rds.logical_replication": "on",
                        "schema": "public",
                        "users": {
                            "root": {
                                "rolname": "postgres",
                                "rolcanlogin": True,
                                "rolcreaterole": True,
                                "rolinherit": True,
                                "rolsuper": True,
                                "memberof": ["rds_superuser"],
                            },
                            "owner": {
                                "rolname": "app_owner",
                                "rolcanlogin": True,
                                "rolcreaterole": False,
                                "rolinherit": True,
                                "rolsuper": False,
                                "memberof": [],
                                "can_create": True,
                            },
                        },
                        "tables": [],
                        "sequences": [],
                        "extensions": [
                            {"extname": "pglogical"},
                            {"extname": "uuid-ossp"},
                        ],
                    },
                }
            ],
            success=True,
            duration_ms=500,
        )
        result = PrecheckResult.model_validate_json(output)
        assert result.command == "precheck"
        assert result.src is not None
        assert result.dst is not None
        assert result.src.root_ok is True
        assert result.src.shared_preload_ok is True
        assert result.extensions_match is True
        assert len(result.src.tables) == 1
        assert result.src.tables[0].replication_method == "pglogical"
        assert result.src.tables[0].has_primary_key is True
        assert len(result.src.sequences) == 1

    def test_error_falls_back_to_generic(self):
        """When an error occurs, even rich commands use generic CommandResult."""
        output = _build_json_output(
            command_name="status",
            dc="dc1",
            db="db1",
            results=[],
            success=False,
            duration_ms=50,
            error=ConnectionError("cannot reach db"),
        )
        parsed = json.loads(output)
        assert parsed["success"] is False
        assert parsed["error"]["error_type"] == "ConnectionError"
        assert parsed["command"] == "status"
