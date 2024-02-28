import re
import subprocess
from time import sleep
from unittest.mock import AsyncMock
from unittest.mock import Mock
from pgbelt.util.dump import _parse_dump_commands

import pgbelt
import pytest


async def _check_status(config, src_dst_status, dst_src_status):
    # Sleep 1, repeat until target status is seen.
    pgbelt.cmd.status.echo = Mock()
    not_replicating = True
    i = 4
    while not_replicating and i > 0:
        sleep(1)
        await pgbelt.cmd.status.status(db=None, dc=config.dc)

        status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

        # Regex for the two columns to be in the correct state
        matches = re.findall(
            rf"^\S+\s+\S+{src_dst_status}\S+\s+\S+{dst_src_status}.*",
            status_echo_call_arg.split("\n")[2],
        )
        if len(matches) == 1:
            not_replicating = False
        elif i > 0:
            i = i - 1
        else:
            raise AssertionError(
                f"Timed out waiting for src->dst: {src_dst_status}, dst->src: {dst_src_status} state. Ended with: {status_echo_call_arg}"
            )


async def _test_check_connectivity(config):
    # Run check_connectivity and make sure all green, no red
    pgbelt.cmd.convenience.echo = Mock()
    await pgbelt.cmd.convenience.check_connectivity(db=config.db, dc=config.dc)
    check_connectivity_echo_call_arg = pgbelt.cmd.convenience.echo.call_args[0][0]
    assert "\x1b[31m" not in check_connectivity_echo_call_arg

    await _check_status(config, "unconfigured", "unconfigured")


async def _test_precheck(config):
    # Run precheck and make sure all green, no red
    pgbelt.cmd.preflight.echo = Mock()
    await pgbelt.cmd.preflight.precheck(db=config.db, dc=config.dc)
    preflight_echo_call_arg = pgbelt.cmd.preflight.echo.call_args[0][0]
    assert "\x1b[31m" not in preflight_echo_call_arg

    await _check_status(config, "unconfigured", "unconfigured")


async def _test_setup(config):
    # Run Setup
    await pgbelt.cmd.setup.setup(db=config.db, dc=config.dc)

    # Ensure Schema in the destination doesn't have NOT VALID, no Indexes
    p = subprocess.Popen(
        ["pg_dump", "-s", config.dst.root_dsn],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()

    commands_raw = _parse_dump_commands(out.decode("utf-8"))
    for c in commands_raw:
        assert "NOT VALID" not in c
        assert "INDEX" not in c

    await _check_status(config, "replicating", "unconfigured")


async def _test_setup_back_replication(config):
    # Set up back replication
    await pgbelt.cmd.setup.setup_back_replication(db=config.db, dc=config.dc)

    await _check_status(config, "replicating", "replicating")


async def _test_create_indexes(config):
    # Load in Indexes
    await pgbelt.cmd.schema.create_indexes(db=config.db, dc=config.dc)

    # Ensure Schema in the destination has Indexes
    p = subprocess.Popen(
        ["pg_dump", "-s", config.dst.root_dsn],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()

    commands_raw = _parse_dump_commands(out.decode("utf-8"))
    index_exists = False
    for c in commands_raw:
        if "INDEX" in c:
            index_exists = True
            break
    assert index_exists

    await _check_status(config, "replicating", "replicating")


async def _test_analyze(config):
    await pgbelt.cmd.sync.analyze(db=config.db, dc=config.dc)

    await _check_status(config, "replicating", "replicating")


async def _test_revoke_logins(config):
    await pgbelt.cmd.login.revoke_logins(db=config.db, dc=config.dc)

    await _check_status(config, "replicating", "replicating")


async def _test_teardown_forward_replication(config):
    await pgbelt.cmd.teardown.teardown_forward_replication(db=config.db, dc=config.dc)

    await _check_status(config, "unconfigured", "replicating")


async def _test_sync(config):
    await pgbelt.cmd.sync.sync(db=config.db, dc=config.dc)

    await _check_status(config, "unconfigured", "replicating")


async def _ensure_same_data(config):
    # Dump the databases and ensure they're the same
    # Unfortunately except for the sequence lines because for some reason, the dump in the source is_called is true, yet on the destination is false.
    # Verified in the code we set it with is_called=True, so not sure what's going on there.
    # ------------------------------------------------------------------

    p = subprocess.Popen(
        ["pg_dump", config.src.root_dsn],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()

    keywords_to_exclude = [
        "EXTENSION ",
        "GRANT ",
        "REVOKE ",
        "setval",
        "SET ",
        "SELECT pg_catalog.set_config('search_path'",
        "ALTER SCHEMA",
        "CREATE SCHEMA",
    ]

    commands_raw = _parse_dump_commands(out.decode("utf-8"))
    commands = []
    for c in commands_raw:
        add_command = True
        for k in keywords_to_exclude:
            if k in c:
                add_command = False
                break
        if add_command:
            commands.append(c)
    source_dump = "\n".join(commands)

    p = subprocess.Popen(
        ["pg_dump", config.dst.root_dsn],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()

    commands_raw = _parse_dump_commands(out.decode("utf-8"))
    commands = []
    for c in commands_raw:
        add_command = True
        for k in keywords_to_exclude:
            if k in c:
                add_command = False
                break
        if add_command:
            commands.append(c)
    dest_dump = "\n".join(commands)

    assert source_dump == dest_dump


async def _test_teardown_not_full(config):
    await pgbelt.cmd.teardown.teardown(db=config.db, dc=config.dc)

    await _check_status(config, "unconfigured", "unconfigured")


async def _test_teardown_full(config):
    await pgbelt.cmd.teardown.teardown(db=config.db, dc=config.dc, full=True)

    await _check_status(config, "unconfigured", "unconfigured")


async def _test_main_workflow(config):
    """
    Run the following commands in order:

    belt check-connectivity testdc && \
    belt precheck testdc && \
    belt setup testdc && \
    belt setup-back-replication testdc && \
    belt create-indexes testdc && \
    belt analyze testdc && \
    belt revoke-logins testdc && \
    belt sync testdc && \
    belt teardown testdc && \
    belt teardown testdc --full
    """

    await _test_check_connectivity(config)
    await _test_precheck(config)
    await _test_setup(config)
    await _test_setup_back_replication(config)
    await _test_create_indexes(config)
    await _test_analyze(config)
    await _test_revoke_logins(config)
    await _test_teardown_forward_replication(config)
    await _test_sync(config)

    # Check if the data is the same before testing teardown
    await _ensure_same_data(config)

    await _test_teardown_not_full(config)
    await _test_teardown_full(config)


# Run the main integration test with objects in the public schema
@pytest.mark.asyncio
async def test_main_workflow_public_schema(setup_db_upgrade_configs):

    await _test_main_workflow(setup_db_upgrade_configs["public-full"])


# Run the main integration test with objects in a non-public schema
@pytest.mark.asyncio
async def test_main_workflow_non_public_schema(
    setup_db_upgrade_configs,
):

    await _test_main_workflow(setup_db_upgrade_configs["nonpublic-full"])


# TODO: fix up the exodus-style tests, the dump comparision step obviously fails because not all data was moved.
# # Run the main integration test with objects in a non-public schema and exodus-style (moving a subset of data)
# @pytest.mark.asyncio
# async def test_main_workflow_public_schema_exodus(setup_db_upgrade_config_public_schema_exodus):

#     await _test_main_workflow(setup_db_upgrade_config_public_schema_exodus)

# TODO: fix up the exodus-style tests, the dump comparision step obviously fails because not all data was moved.
# # Run the main integration test with objects in a non-public schema and exodus-style (moving a subset of data)
# @pytest.mark.asyncio
# async def test_main_workflow_non_public_schema_exodus(setup_db_upgrade_config_non_public_schema_exodus):

#     await _test_main_workflow(setup_db_upgrade_config_non_public_schema_exodus)
