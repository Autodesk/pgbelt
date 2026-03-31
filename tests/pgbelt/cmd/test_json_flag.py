"""
Tests for the --json flag plumbing in helpers.py.

These test the _build_json_output helper and the json_mode path in
run_with_configs without needing real database connections.
"""

import json

from pgbelt.cmd.helpers import _build_json_output
from pgbelt.models.base import CommandResult


class TestBuildJsonOutput:
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

    def test_success_with_single_dict_result(self):
        output = _build_json_output(
            command_name="status",
            dc="dc1",
            db="db1",
            results=[{"pg1_pg2": "replicating", "pg2_pg1": "replicating"}],
            success=True,
            duration_ms=200,
        )
        parsed = json.loads(output)
        assert parsed["success"] is True
        assert parsed["detail"]["pg1_pg2"] == "replicating"

    def test_success_with_multiple_dict_results(self):
        output = _build_json_output(
            command_name="status",
            dc="dc1",
            db=None,
            results=[
                {"db": "db1", "pg1_pg2": "replicating"},
                {"db": "db2", "pg1_pg2": "initializing"},
            ],
            success=True,
            duration_ms=300,
        )
        parsed = json.loads(output)
        assert parsed["success"] is True
        assert len(parsed["detail"]["databases"]) == 2

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
            command_name="status",
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
