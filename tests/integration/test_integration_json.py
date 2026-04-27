"""
Integration test that mirrors test_integration.py but runs every command with
``json_mode=True``.  The goal is to verify that the --json flag produces valid,
parseable JSON for each command throughout the full migration workflow.
"""

import json
import subprocess
from io import StringIO
from time import sleep
from unittest.mock import patch

from pgbelt.util.dump import _parse_dump_commands
from pgbelt.config.models import DbupgradeConfig

import asyncio

from asyncpg import create_pool

import pgbelt
import pytest


def _capture_json(raw: str) -> dict:
    """Parse the JSON blob printed by json_mode and return it as a dict."""
    parsed = json.loads(raw)
    assert isinstance(parsed, dict), f"Expected JSON object, got {type(parsed)}"
    return parsed


async def _run_json_command(coro) -> dict:
    """
    Await *coro* while capturing stdout.  Return the parsed JSON dict.
    The json_mode code path calls ``print()``, so we redirect stdout.
    """
    buf = StringIO()
    with patch("sys.stdout", buf):
        await coro
    output = buf.getvalue().strip()
    assert output, "json_mode produced no stdout"
    return _capture_json(output)


async def _check_status_json(
    configs: dict[str, DbupgradeConfig], src_dst_status: str, dst_src_status: str
):
    dc = list(configs.values())[0].dc
    num_configs = len(configs)

    status_reached = False
    retries = 4
    while not status_reached and retries > 0:
        sleep(1)
        result = await _run_json_command(
            pgbelt.cmd.status.status(db=None, dc=dc, json_mode=True)
        )

        assert "results" in result, f"status JSON missing 'results': {result.keys()}"
        rows = result["results"]

        matches = [
            r
            for r in rows
            if r.get("forward_replication") == src_dst_status
            and r.get("back_replication") == dst_src_status
        ]
        if len(matches) == num_configs:
            status_reached = True
        else:
            retries -= 1
            if retries == 0:
                raise AssertionError(
                    f"Timed out waiting for src->dst: {src_dst_status}, "
                    f"dst->src: {dst_src_status} across {num_configs} configs. "
                    f"Last JSON: {json.dumps(result, indent=2)}"
                )


async def _test_check_connectivity_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.convenience.check_connectivity(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"check-connectivity failed: {result}"
    assert "results" in result

    await _check_status_json(configs, "unconfigured", "unconfigured")


async def _test_precheck_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.preflight.precheck(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"precheck failed: {result}"

    await _check_status_json(configs, "unconfigured", "unconfigured")


async def _test_setup_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.setup.setup(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"setup failed: {result}"

    dst_dumps = await _get_dumps(configs)

    for setname, stdout in dst_dumps.items():
        commands_raw = _parse_dump_commands(stdout.decode("utf-8"))
        for c in commands_raw:
            assert "NOT VALID" not in c
            if "INDEX" in c:
                assert (
                    "UNIQUE" in c
                ), f"Non-unique index found in destination after setup for {setname}: {c}"

    await _check_status_json(configs, "replicating", "unconfigured")


async def _test_setup_back_replication_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.setup.setup_back_replication(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"setup-back-replication failed: {result}"

    await _check_status_json(configs, "replicating", "replicating")


async def _test_create_indexes_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.schema.create_indexes(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"create-indexes failed: {result}"

    dst_dumps = await _get_dumps(configs)

    for setname, stdout in dst_dumps.items():
        commands_raw = _parse_dump_commands(stdout.decode("utf-8"))
        index_exists = any("INDEX" in c for c in commands_raw)
        assert index_exists, f"No INDEX found in destination for {setname}"

    await _check_status_json(configs, "replicating", "replicating")


async def _test_analyze_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.sync.analyze(db=None, dc=dc, json_mode=True)
    )

    assert result.get("success") is True, f"analyze failed: {result}"

    await _check_status_json(configs, "replicating", "replicating")


async def _test_revoke_logins_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc
    first_config = configs[list(configs.keys())[0]]

    async with create_pool(first_config.src.root_uri, min_size=1) as pool:
        for name in ["appuser_alpha", "appuser_beta", "svc_monitor"]:
            await pool.execute(f"CREATE ROLE {name} LOGIN PASSWORD 'testpw';")

        pre_rows = await pool.fetch(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles;"
        )
        pre_login = {r["rolname"]: r["rolcanlogin"] for r in pre_rows}

    # Phase 1: revoke with excludes
    for c in configs.values():
        c.exclude_users = ["owner"]
        await c.save()

    result = await _run_json_command(
        pgbelt.cmd.login.revoke_logins(
            db=None,
            dc=dc,
            json_mode=True,
            exclude_users=[],
            exclude_patterns=["%appuser%"],
        )
    )
    assert result.get("success") is True, f"revoke-logins failed: {result}"

    async with create_pool(first_config.src.root_uri, min_size=1) as pool:
        post_revoke = await pool.fetch(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles;"
        )
        login_status = {r["rolname"]: r["rolcanlogin"] for r in post_revoke}

    assert login_status["svc_monitor"] is False, "svc_monitor should be revoked"
    assert login_status["owner"] is True, "owner excluded by config, should keep login"
    assert (
        login_status["appuser_alpha"] is True
    ), "appuser_alpha excluded by pattern, should keep login"
    assert (
        login_status["appuser_beta"] is True
    ), "appuser_beta excluded by pattern, should keep login"

    for role_name in ["postgres", "pglogical"]:
        assert login_status.get(role_name) == pre_login.get(
            role_name
        ), f"{role_name} should not have been touched by revoke"

    # Phase 2: restore
    result = await _run_json_command(
        pgbelt.cmd.login.restore_logins(db=None, dc=dc, json_mode=True)
    )
    assert result.get("success") is True, f"restore-logins failed: {result}"

    async with create_pool(first_config.src.root_uri, min_size=1) as pool:
        post_restore = await pool.fetch(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles;"
        )
        login_status = {r["rolname"]: r["rolcanlogin"] for r in post_restore}

    for role_name, had_login in pre_login.items():
        if had_login:
            assert (
                login_status.get(role_name) is True
            ), f"{role_name} had login before revoke but not after restore"

    # Phase 3: plain revoke for the rest of the workflow
    for c in configs.values():
        c.exclude_users = None
        await c.save()

    result = await _run_json_command(
        pgbelt.cmd.login.revoke_logins(
            db=None, dc=dc, json_mode=True, exclude_users=[], exclude_patterns=[]
        )
    )
    assert result.get("success") is True, f"revoke-logins (phase 3) failed: {result}"

    await _check_status_json(configs, "replicating", "replicating")


async def _test_teardown_forward_replication_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.teardown.teardown_forward_replication(db=None, dc=dc, json_mode=True)
    )
    assert (
        result.get("success") is True
    ), f"teardown-forward-replication failed: {result}"

    await _check_status_json(configs, "unconfigured", "replicating")


async def _test_sync_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.sync.sync(db=None, dc=dc, json_mode=True)
    )
    assert result.get("success") is True, f"sync failed: {result}"

    await _check_status_json(configs, "unconfigured", "replicating")


async def _test_teardown_not_full_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.teardown.teardown(db=None, dc=dc, json_mode=True)
    )
    assert result.get("success") is True, f"teardown (not full) failed: {result}"

    await _check_status_json(configs, "unconfigured", "unconfigured")


async def _test_teardown_full_json(configs: dict[str, DbupgradeConfig]):
    dc = list(configs.values())[0].dc

    result = await _run_json_command(
        pgbelt.cmd.teardown.teardown(db=None, dc=dc, full=True, json_mode=True)
    )
    assert result.get("success") is True, f"teardown --full failed: {result}"

    await _check_status_json(configs, "unconfigured", "unconfigured")


# ---- Reused helpers from test_integration.py ----


async def _get_dumps(
    configs: dict[str, DbupgradeConfig], src: bool = False
) -> dict[str, str]:
    std_kwargs = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }

    if src:
        dump_processes = await asyncio.gather(
            *[
                asyncio.create_subprocess_exec(
                    "pg_dump",
                    configs[setname].src.root_dsn,
                    **std_kwargs,
                )
                for setname in configs.keys()
            ]
        )
    else:
        dump_processes = await asyncio.gather(
            *[
                asyncio.create_subprocess_exec(
                    "pg_dump",
                    configs[setname].dst.root_dsn,
                    **std_kwargs,
                )
                for setname in configs.keys()
            ]
        )

    await asyncio.gather(*[d.wait() for d in dump_processes])

    return {
        setname: (await d.communicate())[0]
        for setname, d in zip(configs.keys(), dump_processes)
    }


async def _filter_dump(dump: str, keywords_to_exclude: list[str]):
    commands_raw = _parse_dump_commands(dump)
    commands = []
    for c in commands_raw:
        add_command = True
        for k in keywords_to_exclude:
            if k in c:
                add_command = False
                break
        if add_command:
            commands.append(c)
    return "\n".join(commands)


async def _compare_sequences(
    sequences: str, src_root_dsn: str, dst_root_dsn: str, schema_name: str
):
    std_kwargs = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }
    src_seq_fetch_processes = await asyncio.gather(
        *[
            asyncio.create_subprocess_exec(
                "psql",
                src_root_dsn,
                "-c",
                f"'SELECT last_value FROM {schema_name}.\"{sequence}\";'",
                "-t",
                **std_kwargs,
            )
            for sequence in sequences
        ]
    )
    dst_seq_fetch_processes = await asyncio.gather(
        *[
            asyncio.create_subprocess_exec(
                "psql",
                dst_root_dsn,
                "-c",
                f"'SELECT last_value FROM {schema_name}.\"{sequence}\";'",
                "-t",
                **std_kwargs,
            )
            for sequence in sequences
        ]
    )

    await asyncio.gather(*[p.wait() for p in src_seq_fetch_processes])
    await asyncio.gather(*[p.wait() for p in dst_seq_fetch_processes])

    for i in range(len(sequences)):
        src_val = (await src_seq_fetch_processes[i].communicate())[0].strip()
        dst_val = (await dst_seq_fetch_processes[i].communicate())[0].strip()
        assert src_val == dst_val


async def _ensure_same_data(configs: dict[str, DbupgradeConfig]):
    src_dumps = await _get_dumps(configs, src=True)
    dst_dumps = await _get_dumps(configs)

    keywords_to_exclude = [
        "EXTENSION ",
        "GRANT ",
        "REVOKE ",
        "setval",
        "SET ",
        "SELECT pg_catalog.set_config('search_path'",
        "ALTER SCHEMA",
        "CREATE SCHEMA",
        "\\unrestrict",
    ]

    src_dumps_filtered = await asyncio.gather(
        *[
            _filter_dump(dump.decode("utf-8"), keywords_to_exclude)
            for dump in src_dumps.values()
        ]
    )

    dst_dumps_filtered = await asyncio.gather(
        *[
            _filter_dump(dump.decode("utf-8"), keywords_to_exclude)
            for dump in dst_dumps.values()
        ]
    )

    for i in range(len(src_dumps_filtered)):
        setname = list(configs.keys())[i]

        if "exodus" in setname:
            src_dump = src_dumps_filtered[i]
            dst_dump = dst_dumps_filtered[i]

            src_table_data = {}
            for table in configs[setname].tables:
                src_table_data[table] = ""
                for line in src_dump.split("\n"):
                    if f"COPY {configs[setname].schema_name}.{table}" in line:
                        src_table_data[table] = src_table_data[table] + line + "\n"
                    elif len(src_table_data[table]) > 0:
                        src_table_data[table] = src_table_data[table] + line + "\n"
                        if line == "\\.":
                            break
            dst_table_data = {}
            for table in configs[setname].tables:
                dst_table_data[table] = ""
                for line in dst_dump.split("\n"):
                    if f"COPY {configs[setname].schema_name}.{table}" in line:
                        dst_table_data[table] = dst_table_data[table] + line + "\n"
                    elif len(dst_table_data[table]) > 0:
                        dst_table_data[table] = dst_table_data[table] + line + "\n"
                        if line == "\\.":
                            break

            for table in configs[setname].tables:
                assert src_table_data[table] == dst_table_data[table]

            _compare_sequences(
                configs[setname].sequences,
                configs[setname].src.root_dsn,
                configs[setname].dst.root_dsn,
                configs[setname].schema_name,
            )

        else:
            assert src_dumps_filtered[i] == dst_dumps_filtered[i]

            sequences = (
                subprocess.run(
                    [
                        "psql",
                        f'"{configs[setname].src.root_dsn}"',
                        "-c",
                        f"'SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = \"{configs[setname].schema_name}\";'",
                        "-t",
                    ],
                    capture_output=True,
                )
                .stdout.decode("utf-8")
                .strip()
                .split("\n")
            )

            _compare_sequences(
                sequences,
                configs[setname].src.root_dsn,
                configs[setname].dst.root_dsn,
                configs[setname].schema_name,
            )


# ---- Main workflow ----


async def _test_main_workflow_json(configs: dict[str, DbupgradeConfig]):
    """
    Run the full migration workflow with --json on every command:

    belt check-connectivity testdc --json && \\
    belt precheck testdc --json && \\
    belt setup testdc --json && \\
    belt setup-back-replication testdc --json && \\
    belt create-indexes testdc --json && \\
    belt analyze testdc --json && \\
    belt revoke-logins testdc --json && \\
    belt sync testdc --json && \\
    belt teardown testdc --json && \\
    belt teardown testdc --full --json
    """

    await _test_check_connectivity_json(configs)
    await _test_precheck_json(configs)
    await _test_setup_json(configs)
    await _test_setup_back_replication_json(configs)
    await _test_create_indexes_json(configs)
    await _test_analyze_json(configs)
    await _test_revoke_logins_json(configs)
    await _test_teardown_forward_replication_json(configs)
    await _test_sync_json(configs)

    await _ensure_same_data(configs)

    await _test_teardown_not_full_json(configs)
    await _test_teardown_full_json(configs)


@pytest.mark.asyncio
async def test_main_workflow_json(setup_db_upgrade_configs):

    await _test_main_workflow_json(setup_db_upgrade_configs)
