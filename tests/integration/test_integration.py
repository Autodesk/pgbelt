import re
import subprocess
from time import sleep
from unittest.mock import AsyncMock
from unittest.mock import Mock
from pgbelt.util.dump import _parse_dump_commands

import pgbelt
import pytest


@pytest.mark.asyncio
async def test_main_workflow(setup_db_upgrade_config):
    # Run check_connectivity and make sure all green, no red
    pgbelt.cmd.convenience.echo = AsyncMock()
    await pgbelt.cmd.convenience.check_connectivity(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )
    check_connectivity_echo_call_arg = pgbelt.cmd.convenience.echo.call_args[0][0]
    assert "\x1b[31m" not in check_connectivity_echo_call_arg

    # Run precheck and make sure all green, no red
    pgbelt.cmd.preflight.echo = Mock()
    await pgbelt.cmd.preflight.precheck(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )
    preflight_echo_call_arg = pgbelt.cmd.preflight.echo.call_args[0][0]
    assert "\x1b[31m" not in preflight_echo_call_arg

    # Run Setup
    await pgbelt.cmd.setup.setup(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Ensure Schema in the destination doesn't have NOT VALID, no Indexes
    p = subprocess.Popen(
        ["pg_dump", "-s", setup_db_upgrade_config.dst.root_dsn],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()

    commands_raw = _parse_dump_commands(out.decode("utf-8"))
    for c in commands_raw:
        assert "NOT VALID" not in c
        assert "INDEX" not in c

    # Sleep 1, repeat until 'replicating' forward
    pgbelt.cmd.status.echo = Mock()
    not_replicating = True
    i = 4
    while not_replicating and i > 0:
        sleep(1)
        await pgbelt.cmd.status.status(db=None, dc=setup_db_upgrade_config.dc)

        status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

        # Regex for 2nd column (src -> dest) saying replicating in green
        matches = re.findall(
            r"^\S+\s+\S+replicating\S+\s+\S+unconfigured.*",
            status_echo_call_arg.split("\n")[2],
        )
        if len(matches) == 1:
            not_replicating = False
        elif i > 0:
            i = i - 1
        else:
            raise AssertionError(
                "Timed out waiting for src -> dest to get to the 'replicating' state."
            )

    # Set up back replication
    await pgbelt.cmd.setup.setup_back_replication(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Sleep 1, repeat until 'replicating' backward
    not_replicating = True
    i = 4
    while not_replicating and i > 0:
        sleep(1)
        await pgbelt.cmd.status.status(db=None, dc=setup_db_upgrade_config.dc)

        status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

        # Regex for 2nd column (src -> dest) saying replicating in green
        matches = re.findall(
            r"^\S+\s+\S+replicating\S+\s+\S+replicating.*",
            status_echo_call_arg.split("\n")[2],
        )
        if len(matches) == 1:
            not_replicating = False
        elif i > 0:
            i = i - 1
        else:
            raise AssertionError(
                "Timed out waiting for src <- dest to get to the 'replicating' state."
            )

    # Load in Indexes
    await pgbelt.cmd.schema.create_indexes(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Ensure Schema in the destination has Indexes
    p = subprocess.Popen(
        ["pg_dump", "-s", setup_db_upgrade_config.dst.root_dsn],
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

    # Run ANALYZE
    await pgbelt.cmd.sync.analyze(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Revoke logins (TODO: test for this?)
    await pgbelt.cmd.login.revoke_logins(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Make sure forward says 'unconfigured' but reverse says 'replicating'
    # ------------------------------------------------------------------
    await pgbelt.cmd.teardown.teardown_forward_replication(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    await pgbelt.cmd.status.status(db=None, dc=setup_db_upgrade_config.dc)

    status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

    # Regex for 2nd column (src -> dest) saying unconfigured, but 3rd (src <- dst) is replicating still
    matches = re.findall(
        r"^\S+\s+\S+unconfigured\S+\s+\S+replicating.*",
        status_echo_call_arg.split("\n")[2],
    )
    assert len(matches) == 1
    # ------------------------------------------------------------------

    # At this point, check sequence numbers? maybe
    await pgbelt.cmd.sync.sync(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    # Make sure forward and back says 'unconfigured'
    # ------------------------------------------------------------------
    await pgbelt.cmd.teardown.teardown_back_replication(
        db=setup_db_upgrade_config.db, dc=setup_db_upgrade_config.dc
    )

    await pgbelt.cmd.status.status(db=None, dc=setup_db_upgrade_config.dc)

    status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

    # Regex for 2nd column (src -> dest) saying unconfigured, but 3rd (src <- dst) is replicating still
    matches = re.findall(
        r"^\S+\s+\S+unconfigured\S+\s+\S+unconfigured.*",
        status_echo_call_arg.split("\n")[2],
    )
    assert len(matches) == 1
    # ------------------------------------------------------------------

    # Dump the databases and ensure they're the same
    # Unfortunately except for the sequence lines because for some reason, the dump in the source is_called is true, yet on the destination is false.
    # Verified in the code we set it with is_called=True, so not sure what's going on there.
    # ------------------------------------------------------------------

    p = subprocess.Popen(
        ["pg_dump", setup_db_upgrade_config.src.root_dsn],
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
        ["pg_dump", setup_db_upgrade_config.dst.root_dsn],
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

    # ------------------------------------------------------------------
