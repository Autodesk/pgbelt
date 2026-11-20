"""Unit tests for extension-owned tables (STOPS-8345): tables created by an
extension, such as PostGIS's spatial_ref_sys or Aurora's apg_plan_mgmt.plans,
are copied by sync-tables instead of failing precheck on their owner."""

import logging
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from pgbelt.cmd import sync as sync_cmd
from pgbelt.cmd.helpers import _build_precheck_result
from pgbelt.cmd.helpers import _build_precheck_side
from pgbelt.util import dump
from pgbelt.util.postgres import quote_ident
from pgbelt.util.postgres import table_empty


@pytest.fixture
def logger():
    return logging.getLogger("test.extension-tables")


SPATIAL_REF_SYS = {
    "schema": "public",
    "name": "spatial_ref_sys",
    "extension": "postgis",
    "condition": "WHERE NOT (srid BETWEEN 2000 AND 2217)",
}
QPM_PLANS = {
    "schema": "apg_plan_mgmt",
    "name": "plans",
    "extension": "apg_plan_mgmt",
    "condition": None,
}


def _raw_side(tables, schema="public", owner="app_owner"):
    return {
        "db": "db1",
        "schema": schema,
        "server_version": "14.22",
        "max_replication_slots": "10",
        "max_worker_processes": "10",
        "max_wal_senders": "10",
        "shared_preload_libraries": "pg_stat_statements,pglogical",
        "rds.logical_replication": "on",
        "users": {
            "root": {
                "rolname": "postgres",
                "rolcanlogin": True,
                "rolcreaterole": True,
                "rolinherit": True,
                "rolsuper": False,
                "memberof": ["rds_superuser"],
            },
            "owner": {
                "rolname": owner,
                "rolcanlogin": True,
                "rolcreaterole": False,
                "rolinherit": True,
                "rolsuper": False,
                "memberof": [],
                "can_create": True,
            },
        },
        "tables": tables,
        "sequences": [],
        "extensions": [{"extname": "postgis"}, {"extname": "apg_plan_mgmt"}],
    }


def _table(name, schema="public", owner="app_owner", extension=None, **kw):
    return {
        "Name": name,
        "Schema": schema,
        "Owner": owner,
        "Extension": extension,
        "HasPrimaryKey": kw.get("has_pk", True),
        "RootCanSelect": kw.get("can_select", True),
        "RootCanInsert": kw.get("can_insert", True),
    }


# ---------------------------------------------------------------------------
# Precheck classification
# ---------------------------------------------------------------------------


class TestPrecheckClassification:
    def test_extension_tables_are_dump_and_load_whatever_owner_and_schema(self):
        src_tables = [
            _table("users"),
            _table("spatial_ref_sys", owner="rdsadmin", extension="postgis"),
            _table(
                "plans",
                schema="apg_plan_mgmt",
                owner="apg_plan_mgmt",
                extension="apg_plan_mgmt",
            ),
        ]
        side = _build_precheck_side(
            _raw_side(src_tables),
            pkeys=["users"],
            other_side_tables=[dict(t) for t in src_tables],
        )
        by_name = {t.name: t for t in side.tables}
        assert by_name["users"].replication_method == "pglogical"
        assert by_name["users"].extension is None
        for name, ext in [("spatial_ref_sys", "postgis"), ("plans", "apg_plan_mgmt")]:
            assert by_name[name].replication_method == "dump_and_load"
            assert by_name[name].extension == ext
            assert by_name[name].note is None
            # analyze_table_pkeys skips extension tables; the PK comes from
            # the catalog instead.
            assert by_name[name].has_primary_key is True

    def test_regular_table_with_wrong_owner_still_unavailable(self):
        side = _build_precheck_side(
            _raw_side([_table("lock_table", owner="someone_else")]), pkeys=[]
        )
        assert side.tables[0].replication_method == "unavailable"

    def test_extension_table_missing_from_destination(self):
        src_tables = [
            _table("plans", schema="apg_plan_mgmt", extension="apg_plan_mgmt")
        ]
        side = _build_precheck_side(
            _raw_side(src_tables), pkeys=[], other_side_tables=[]
        )
        t = side.tables[0]
        assert t.replication_method == "unavailable"
        assert "not found in the destination" in t.note
        assert "apg_plan_mgmt extension" in t.note

    def test_extension_table_root_cannot_write_destination(self):
        src = [_table("plans", schema="apg_plan_mgmt", extension="apg_plan_mgmt")]
        dst = [
            _table(
                "plans",
                schema="apg_plan_mgmt",
                extension="apg_plan_mgmt",
                can_insert=False,
            )
        ]
        side = _build_precheck_side(_raw_side(src), pkeys=[], other_side_tables=dst)
        assert side.tables[0].replication_method == "unavailable"
        assert "cannot write" in side.tables[0].note

    def test_extension_table_root_cannot_read_source(self):
        src = [_table("spatial_ref_sys", extension="postgis", can_select=False)]
        side = _build_precheck_side(_raw_side(src), pkeys=[], other_side_tables=src)
        assert side.tables[0].replication_method == "unavailable"
        assert "cannot read" in side.tables[0].note

    def test_pglogical_extension_tables_are_not_treated_as_extension_tables(self):
        side = _build_precheck_side(
            _raw_side([_table("node", schema="pglogical", extension="pglogical")]),
            pkeys=[],
        )
        assert side.tables[0].extension is None
        assert side.tables[0].replication_method == "unavailable"

    def test_full_result_uses_destination_tables_and_qpm_counts(self):
        src_tables = [
            _table("plans", schema="apg_plan_mgmt", extension="apg_plan_mgmt")
        ]
        src = _raw_side(src_tables)
        src["pkeys"] = []
        src["qpm_plan_counts"] = {"Approved": 3, "Rejected": 1}
        dst = _raw_side([])  # extension not installed on the destination
        result = _build_precheck_result(
            [{"src": src, "dst": dst}], dict(db="db1", dc="dc1", duration_ms=1)
        )
        assert result.src.tables[0].replication_method == "unavailable"
        assert result.src.qpm_plan_counts == {"Approved": 3, "Rejected": 1}
        assert result.dst.qpm_plan_counts is None

    def test_old_raw_rows_without_extension_columns(self):
        """Rows without the new columns classify exactly as before."""
        raw = _raw_side([{"Name": "t", "Schema": "public", "Owner": "app_owner"}])
        side = _build_precheck_side(raw, pkeys=["t"])
        assert side.tables[0].replication_method == "pglogical"


# ---------------------------------------------------------------------------
# Emptiness check
# ---------------------------------------------------------------------------


class TestTableEmpty:
    @pytest.mark.asyncio
    async def test_condition_is_applied(self, logger):
        pool = AsyncMock()
        pool.fetch.return_value = []
        assert await table_empty(
            pool, "spatial_ref_sys", '"public"', logger, "WHERE srid > 900000"
        )
        sql = pool.fetch.await_args.args[0]
        assert sql == (
            'SELECT 1 FROM "public"."spatial_ref_sys" WHERE srid > 900000 LIMIT 1;'
        )

    @pytest.mark.asyncio
    async def test_without_condition_matches_old_query_shape(self, logger):
        pool = AsyncMock()
        pool.fetch.return_value = [object()]
        assert not await table_empty(pool, "Users", "public", logger)
        assert pool.fetch.await_args.args[0] == 'SELECT 1 FROM public."Users"  LIMIT 1;'


def test_quote_ident():
    assert quote_ident("plans") == '"plans"'
    assert quote_ident('we"ird') == '"we""ird"'


# ---------------------------------------------------------------------------
# Loading extension tables
# ---------------------------------------------------------------------------


class _FakePool:
    """Stand-in for an asyncpg pool used as an async context manager."""

    def __init__(self, name):
        self.name = name
        self.execute = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def pools(config):
    src, dst = _FakePool("src"), _FakePool("dst")

    def _create_pool(uri, **kwargs):
        return src if uri == config.src.root_uri else dst

    with patch("pgbelt.util.dump.create_pool", new=_create_pool):
        yield src, dst


def _columns(src_cols, dst_cols):
    async def _get(pool, schema, table):
        return src_cols if pool.name == "src" else dst_cols

    return _get


class TestLoadExtensionTables:
    @pytest.mark.asyncio
    async def test_loads_with_filter_column_intersection_and_qpm_reload(
        self, config, logger, pools
    ):
        _, dst = pools
        empty = AsyncMock(return_value=True)
        pipe = AsyncMock(return_value=4)
        with (
            patch(
                "pgbelt.util.dump.get_table_columns",
                new=_columns(
                    ["sql_hash", "plan_hash", "old_col"], ["sql_hash", "plan_hash"]
                ),
            ),
            patch("pgbelt.util.dump.table_empty", new=empty),
            patch("pgbelt.util.dump._pipe_copy_extension_table", new=pipe),
        ):
            details = await dump.load_extension_tables_with_details(
                config, [QPM_PLANS], logger
            )

        assert details[0]["loaded"] is True
        assert details[0]["row_count"] == 4
        assert details[0]["schema_name"] == "apg_plan_mgmt"
        assert details[0]["extension"] == "apg_plan_mgmt"
        assert "error" not in details[0]
        # Only columns present on both sides are copied.
        assert pipe.await_args.args[2] == ["sql_hash", "plan_hash"]
        # Emptiness is checked in the table's own schema.
        assert empty.await_args.args[1:3] == ("plans", '"apg_plan_mgmt"')
        dst.execute.assert_awaited_once_with("SELECT apg_plan_mgmt.reload();")

    @pytest.mark.asyncio
    async def test_emptiness_uses_extension_filter(self, config, logger, pools):
        empty = AsyncMock(return_value=True)
        with (
            patch(
                "pgbelt.util.dump.get_table_columns", new=_columns(["srid"], ["srid"])
            ),
            patch("pgbelt.util.dump.table_empty", new=empty),
            patch(
                "pgbelt.util.dump._pipe_copy_extension_table",
                new=AsyncMock(return_value=0),
            ),
        ):
            details = await dump.load_extension_tables_with_details(
                config, [SPATIAL_REF_SYS], logger
            )
        assert empty.await_args.args[4] == SPATIAL_REF_SYS["condition"]
        assert details[0]["loaded"] is True
        # No reload for tables other than apg_plan_mgmt.plans.
        _, dst = pools
        dst.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_when_destination_has_rows(self, config, logger, pools):
        pipe = AsyncMock()
        with (
            patch("pgbelt.util.dump.get_table_columns", new=_columns(["a"], ["a"])),
            patch("pgbelt.util.dump.table_empty", new=AsyncMock(return_value=False)),
            patch("pgbelt.util.dump._pipe_copy_extension_table", new=pipe),
        ):
            details = await dump.load_extension_tables_with_details(
                config, [QPM_PLANS], logger
            )
        assert details == [
            {
                "name": "plans",
                "schema_name": "apg_plan_mgmt",
                "extension": "apg_plan_mgmt",
                "loaded": False,
                "skipped_reason": "destination table not empty",
            }
        ]
        pipe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_destination_table_is_an_error(self, config, logger, pools):
        with patch("pgbelt.util.dump.get_table_columns", new=_columns(["a"], [])):
            details = await dump.load_extension_tables_with_details(
                config, [QPM_PLANS], logger
            )
        assert details[0]["loaded"] is False
        assert "not found in the destination" in details[0]["error"]

    @pytest.mark.asyncio
    async def test_copy_failure_is_reported(self, config, logger, pools):
        with (
            patch("pgbelt.util.dump.get_table_columns", new=_columns(["a"], ["a"])),
            patch("pgbelt.util.dump.table_empty", new=AsyncMock(return_value=True)),
            patch(
                "pgbelt.util.dump._pipe_copy_extension_table",
                new=AsyncMock(side_effect=Exception("duplicate key")),
            ),
        ):
            details = await dump.load_extension_tables_with_details(
                config, [QPM_PLANS], logger
            )
        assert details[0]["loaded"] is False
        assert details[0]["error"] == "duplicate key"
        _, dst = pools
        dst.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reload_failure_keeps_loaded_and_reports(self, config, logger, pools):
        _, dst = pools
        dst.execute.side_effect = Exception("permission denied for function reload")
        with (
            patch("pgbelt.util.dump.get_table_columns", new=_columns(["a"], ["a"])),
            patch("pgbelt.util.dump.table_empty", new=AsyncMock(return_value=True)),
            patch(
                "pgbelt.util.dump._pipe_copy_extension_table",
                new=AsyncMock(return_value=1),
            ),
        ):
            details = await dump.load_extension_tables_with_details(
                config, [QPM_PLANS], logger
            )
        assert details[0]["loaded"] is True
        assert "apg_plan_mgmt.reload(); failed" in details[0]["error"]

    @pytest.mark.asyncio
    async def test_no_tables_opens_no_connections(self, config, logger):
        with patch("pgbelt.util.dump.create_pool") as cp:
            assert (
                await dump.load_extension_tables_with_details(config, [], logger) == []
            )
        cp.assert_not_called()


class TestPipeCopyExtensionTable:
    @pytest.mark.asyncio
    async def test_command_shape(self, config, logger):
        proc = MagicMock()
        proc.returncode = 0
        proc.communicate = AsyncMock(
            return_value=(b"BEGIN\nSET\nCOPY 7\nCOMMIT\n", b"")
        )
        with patch(
            "pgbelt.util.dump.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=proc),
        ) as spawn:
            rows = await dump._pipe_copy_extension_table(
                config, SPATIAL_REF_SYS, ["srid", "auth_name"], logger
            )
        assert rows == 7
        args = spawn.await_args.args
        assert args[:2] == ("bash", "-c")
        cmd = args[2]
        assert cmd.startswith("set -o pipefail; ")
        assert (
            'COPY (SELECT "srid", "auth_name" FROM "public"."spatial_ref_sys" '
            "WHERE NOT (srid BETWEEN 2000 AND 2217)) TO STDOUT"
        ) in cmd
        assert (
            '\\copy "public"."spatial_ref_sys" ("srid", "auth_name") FROM pstdin' in cmd
        )
        assert "SET LOCAL session_replication_role = replica;" in cmd
        # The source psql is quiet, so no status line lands in the data.
        assert cmd.index("psql -X -q") < cmd.index("|")

    @pytest.mark.asyncio
    async def test_failure_raises(self, config, logger):
        proc = MagicMock()
        proc.returncode = 1
        proc.communicate = AsyncMock(return_value=(b"", b"ERROR: boom"))
        with patch(
            "pgbelt.util.dump.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=proc),
        ):
            with pytest.raises(Exception, match="(?s)apg_plan_mgmt.plans.*boom"):
                await dump._pipe_copy_extension_table(config, QPM_PLANS, ["a"], logger)


# ---------------------------------------------------------------------------
# sync-tables / sync discovery
# ---------------------------------------------------------------------------


class TestSyncDiscovery:
    def test_filter_by_bare_or_qualified_name(self):
        tables = [SPATIAL_REF_SYS, QPM_PLANS]
        assert sync_cmd._filter_extension_tables(tables, ["spatial_ref_sys"]) == [
            SPATIAL_REF_SYS
        ]
        assert sync_cmd._filter_extension_tables(tables, ["apg_plan_mgmt.plans"]) == [
            QPM_PLANS
        ]
        assert sync_cmd._filter_extension_tables(tables, ["users"]) == []

    @pytest.mark.asyncio
    async def test_table_list_limits_extension_tables(self, config, logger):
        config.tables = ["users", "spatial_ref_sys"]
        with patch(
            "pgbelt.cmd.sync.get_extension_tables",
            new=AsyncMock(return_value=[SPATIAL_REF_SYS, QPM_PLANS]),
        ):
            found = await sync_cmd._discover_extension_tables(config, None, logger)
        assert found == [SPATIAL_REF_SYS]

    @pytest.mark.asyncio
    async def test_no_table_list_returns_all(self, config, logger):
        with patch(
            "pgbelt.cmd.sync.get_extension_tables",
            new=AsyncMock(return_value=[SPATIAL_REF_SYS, QPM_PLANS]),
        ):
            found = await sync_cmd._discover_extension_tables(config, None, logger)
        assert found == [SPATIAL_REF_SYS, QPM_PLANS]

    @pytest.mark.asyncio
    async def test_schema_names_copy_extension_tables_once(self, config, logger):
        config.schema_names = ["app", "audit", "reports"]
        with patch(
            "pgbelt.cmd.sync.get_extension_tables",
            new=AsyncMock(return_value=[SPATIAL_REF_SYS, QPM_PLANS]),
        ):
            found = [
                await sync_cmd._discover_extension_tables(m, None, logger)
                for m in config.expand_schemas()
            ]
        assert found == [[SPATIAL_REF_SYS, QPM_PLANS], [], []]

    @pytest.mark.asyncio
    async def test_sync_tables_explicit_skips_extension_tables_after_first_schema(
        self, config
    ):
        config.schema_names = ["app", "audit"]
        _, second = config.expand_schemas()

        async def _config_future():
            return second

        pool = _FakePool("src")
        dump_regular = AsyncMock(return_value=[])
        load_ext = AsyncMock(return_value=[])
        with (
            patch("pgbelt.cmd.sync.create_pool", new=lambda *a, **k: pool),
            patch(
                "pgbelt.cmd.sync.get_extension_tables",
                new=AsyncMock(return_value=[SPATIAL_REF_SYS, QPM_PLANS]),
            ),
            patch(
                "pgbelt.cmd.sync.dump_and_load_tables_with_details", new=dump_regular
            ),
            patch("pgbelt.cmd.sync.load_extension_tables_with_details", new=load_ext),
        ):
            await sync_cmd.sync_tables.__wrapped__(
                _config_future(), table=["nopk", "apg_plan_mgmt.plans"]
            )

        assert dump_regular.await_args.args[1] == ["nopk"]
        assert load_ext.await_args.args[1] == []

    @pytest.mark.asyncio
    async def test_sync_tables_auto_includes_extension_tables(self, config):
        async def _config_future():
            return config

        pool = _FakePool("src")
        dump_regular = AsyncMock(return_value=[{"name": "nopk", "loaded": True}])
        load_ext = AsyncMock(
            return_value=[
                {
                    "name": "plans",
                    "schema_name": "apg_plan_mgmt",
                    "extension": "apg_plan_mgmt",
                    "loaded": True,
                }
            ]
        )
        with (
            patch("pgbelt.cmd.sync.create_pool", new=lambda *a, **k: pool),
            patch(
                "pgbelt.cmd.sync.analyze_table_pkeys",
                new=AsyncMock(return_value=(["users"], ["nopk"], [])),
            ),
            patch(
                "pgbelt.cmd.sync.get_extension_tables",
                new=AsyncMock(return_value=[QPM_PLANS]),
            ),
            patch(
                "pgbelt.cmd.sync.dump_and_load_tables_with_details", new=dump_regular
            ),
            patch("pgbelt.cmd.sync.load_extension_tables_with_details", new=load_ext),
        ):
            result = await sync_cmd.sync_tables.__wrapped__(_config_future(), table=[])

        assert dump_regular.await_args.args[1] == ["nopk"]
        assert load_ext.await_args.args[1] == [QPM_PLANS]
        assert [t["name"] for t in result["tables"]] == ["nopk", "plans"]

    @pytest.mark.asyncio
    async def test_sync_tables_explicit_routes_extension_tables(self, config):
        async def _config_future():
            return config

        pool = _FakePool("src")
        dump_regular = AsyncMock(return_value=[])
        load_ext = AsyncMock(return_value=[])
        with (
            patch("pgbelt.cmd.sync.create_pool", new=lambda *a, **k: pool),
            patch(
                "pgbelt.cmd.sync.get_extension_tables",
                new=AsyncMock(return_value=[SPATIAL_REF_SYS, QPM_PLANS]),
            ),
            patch(
                "pgbelt.cmd.sync.dump_and_load_tables_with_details", new=dump_regular
            ),
            patch("pgbelt.cmd.sync.load_extension_tables_with_details", new=load_ext),
        ):
            result = await sync_cmd.sync_tables.__wrapped__(
                _config_future(), table=["nopk", "apg_plan_mgmt.plans"]
            )

        assert result["discovery_mode"] == "explicit"
        assert dump_regular.await_args.args[1] == ["nopk"]
        assert load_ext.await_args.args[1] == [QPM_PLANS]

    @pytest.mark.asyncio
    async def test_sync_raises_when_an_extension_table_fails(self, config, logger):
        with (
            patch(
                "pgbelt.cmd.sync.analyze_table_pkeys",
                new=AsyncMock(return_value=([], [], [])),
            ),
            patch(
                "pgbelt.cmd.sync.get_extension_tables",
                new=AsyncMock(return_value=[QPM_PLANS]),
            ),
            patch("pgbelt.cmd.sync.dump_and_load_tables", new=AsyncMock()),
            patch(
                "pgbelt.cmd.sync.load_extension_tables_with_details",
                new=AsyncMock(
                    return_value=[
                        {
                            "name": "plans",
                            "schema_name": "apg_plan_mgmt",
                            "loaded": False,
                            "error": "boom",
                        }
                    ]
                ),
            ),
        ):
            with pytest.raises(Exception, match="apg_plan_mgmt.plans: boom"):
                await sync_cmd._dump_and_load_all_tables(config, None, logger, logger)
