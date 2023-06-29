import re
from time import sleep
from unittest.mock import AsyncMock
from unittest.mock import Mock

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
