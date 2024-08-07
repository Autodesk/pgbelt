import re
import subprocess
from time import sleep
from unittest.mock import AsyncMock
from unittest.mock import Mock
from pgbelt.util.dump import _parse_dump_commands
from pgbelt.config.models import DbupgradeConfig

import asyncio
from asyncpg import create_pool

import pgbelt
import pytest


async def _check_status(
    configs: dict[str, DbupgradeConfig], src_dst_status: str, dst_src_status: str
):
    # Check status and make sure all are in the correct state
    # ALL sets must match the src_dst_status and dst_src_status

    dc = list(configs.values())[0].dc
    num_configs = len(configs.keys())

    # Sleep 1, repeat until target status is seen.
    pgbelt.cmd.status.echo = Mock()
    status_reached = False
    i = 4
    while not status_reached and i > 0:
        sleep(1)
        await pgbelt.cmd.status.status(db=None, dc=dc)

        status_echo_call_arg = pgbelt.cmd.status.echo.call_args[0][0]

        # Regex for the two columns to be in the correct state
        matches = re.findall(
            rf"^\S+\s+\S+{src_dst_status}\S+\s+\S+{dst_src_status}.*",
            status_echo_call_arg.split("\n")[2],
        )
        if len(matches) == num_configs:
            status_reached = True
        elif i > 0:
            i = i - 1
        else:
            raise AssertionError(
                f"Timed out waiting for src->dst: {src_dst_status}, dst->src: {dst_src_status} state across {num_configs} configs. Ended with: {status_echo_call_arg}"
            )


async def _test_check_connectivity(configs: dict[str, DbupgradeConfig]):
    # Run check_connectivity and make sure all green, no rec
    pgbelt.cmd.convenience.echo = Mock()
    await pgbelt.cmd.convenience.check_connectivity(
        db=None, dc=configs[list(configs.keys())[0]].dc
    )
    check_connectivity_echo_call_arg = pgbelt.cmd.convenience.echo.call_args[0][0]
    assert "\x1b[31m" not in check_connectivity_echo_call_arg

    await _check_status(configs, "unconfigured", "unconfigured")


async def _test_precheck(configs: dict[str, DbupgradeConfig]):
    # Run precheck and make sure all green, no red
    pgbelt.cmd.preflight.echo = Mock()
    await pgbelt.cmd.preflight.precheck(db=None, dc=configs[list(configs.keys())[0]].dc)
    preflight_echo_call_arg = pgbelt.cmd.preflight.echo.call_args[0][0]
    assert "\x1b[31m" not in preflight_echo_call_arg

    await _check_status(configs, "unconfigured", "unconfigured")


async def _test_setup(configs: dict[str, DbupgradeConfig]):
    # Run Setup on the dc of the first config to run against all DBs in that dc
    await pgbelt.cmd.setup.setup(db=None, dc=configs[list(configs.keys())[0]].dc)

    # Ensure Schema in the destination doesn't have NOT VALID, no Indexes across all DB pairs
    dst_dumps = await _get_dumps(configs)

    # Format of dumps: {setname: stdout}
    for setname, stdout in dst_dumps.items():
        commands_raw = _parse_dump_commands(stdout.decode("utf-8"))
        print(
            f"Test Setup: checking {setname} for NOT VALID and INDEXES in destination schema..."
        )
        for c in commands_raw:
            assert "NOT VALID" not in c
            assert "INDEX" not in c

    await _check_status(configs, "replicating", "unconfigured")


async def _test_setup_back_replication(configs: dict[str, DbupgradeConfig]):
    # Set up back replication
    await pgbelt.cmd.setup.setup_back_replication(
        db=None, dc=configs[list(configs.keys())[0]].dc
    )

    await _check_status(configs, "replicating", "replicating")


async def _test_create_indexes(configs: dict[str, DbupgradeConfig]):
    # Load in Indexes
    await pgbelt.cmd.schema.create_indexes(
        db=None, dc=configs[list(configs.keys())[0]].dc
    )

    # Ensure Schema in the destination has Indexes

    dst_dumps = await _get_dumps(configs)

    # Format of dumps: {setname: stdout}
    for setname, stdout in dst_dumps.items():

        print(
            f"Test Create-Indexes: checking {setname} for INDEXES in destination schema..."
        )

        commands_raw = _parse_dump_commands(stdout.decode("utf-8"))
        index_exists = False
        for c in commands_raw:
            if "INDEX" in c:
                index_exists = True
                break
        assert index_exists

    await _check_status(configs, "replicating", "replicating")


async def _test_analyze(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.sync.analyze(db=None, dc=configs[list(configs.keys())[0]].dc)

    # TODO: test that ANALYZE was run on the destination

    await _check_status(configs, "replicating", "replicating")


async def _test_revoke_logins(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.login.revoke_logins(
        db=None, dc=configs[list(configs.keys())[0]].dc
    )

    # TODO: test that appropriate login roles were revoked

    await _check_status(configs, "replicating", "replicating")


async def _test_teardown_forward_replication(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.teardown.teardown_forward_replication(
        db=None, dc=configs[list(configs.keys())[0]].dc
    )

    await _check_status(configs, "unconfigured", "replicating")


async def _test_sync(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.sync.sync(db=None, dc=configs[list(configs.keys())[0]].dc)

    # TODO: test that the appropriate sync steps were run

    await _check_status(configs, "unconfigured", "replicating")


async def _get_dumps(
    configs: dict[str, DbupgradeConfig], src: bool = False
) -> dict[str, str]:
    """
    Get the full dumps for the source or destination databases using pg_dump.
    Default is destination.
    """

    std_kwargs = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }

    # For each set of DBs, run pg_dump -s against the destination
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
    else:  # Default is destination
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

    # get STDOUT for each dump
    # Format of dumps: {setname: stdout}
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


async def _ensure_same_data(configs: dict[str, DbupgradeConfig]):
    # Dump the databases and ensure they're the same
    # Unfortunately except for the sequence lines because for some reason, the dump in the source is_called is true, yet on the destination is false.
    # Verified in the code we set it with is_called=True, so not sure what's going on there.
    # ------------------------------------------------------------------

    # Get all the SRC and DST Dumps
    # Format of dumps: {setname: stdout}
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
    ]

    # First, asynchronously filter out the keywords from the source dumps

    # Run the filter_dump function on each dump asynchronously
    src_dumps_filtered = await asyncio.gather(
        *[
            _filter_dump(dump.decode("utf-8"), keywords_to_exclude)
            for dump in src_dumps.values()
        ]
    )

    # Then, asynchronously filter out the keywords from the destination dumps
    dst_dumps_filtered = await asyncio.gather(
        *[
            _filter_dump(dump.decode("utf-8"), keywords_to_exclude)
            for dump in dst_dumps.values()
        ]
    )

    # Note: the asyncio gathers will return a list of the filtered dumps in the same order as the input dumps
    # So we can safely say that the ith element of each list corresponds to the same set of DBs

    # Ensure the filtered dumps are the same
    for i in range(len(src_dumps_filtered)):
        setname = list(configs.keys())[i]

        # Only the targeted tables should match in exodus-style migrations
        if "exodus" in setname:

            # In a real exodus migration, only the schema related to the targeted tables will probably exist.
            # But in our integration testing, we just copy the entire schema yet just copy only the targeted data.

            # Given this, the only thing to really check is that the targeted data is the same. Even the schema and structure is not the responsibility of pgbelt.

            src_dump = src_dumps_filtered[i]
            dst_dump = dst_dumps_filtered[i]

            # Only get the COPY lines for the targeted tables in the dumps.
            # COPY format:
            # COPY non_public_schema.users (id, hash_firstname, hash_lastname, gender) FROM stdin;

            # 1	garbagefirst	garbagelast	male
            # 2	garbagefirst1	garbagelast1	female
            # 3	sdgarbagefirst	dgsadsrbagelast	male
            # 4	dsdssdgarbagefirst	dgsaggggdjjjsrbagelast	female
            # 5	dsdssdgarbagefirt	dgsagggdjjjsrbagelast	female
            # \.

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

            # Ensure the targeted data is the same
            for table in configs[setname].tables:
                print(
                    f"Ensuring {setname} source and destination data for table {table} are the same..."
                )

                assert src_table_data[table] == dst_table_data[table]

            # We also need to ensure the sequences are the same
            # I'm using the same code as in the sync_sequences function to do this because it has
            # all the logic to handle exodus-style migrations and target the right sequences.
            src_pool, dst_pool = await asyncio.gather(
                create_pool(configs[setname].src.pglogical_uri, min_size=1),
                create_pool(configs[setname].dst.root_uri, min_size=1),
            )
            src_seq_vals = await pgbelt.util.postgres.dump_sequences(
                src_pool,
                configs[setname].sequences,
                configs[setname].schema_name,
                pgbelt.util.logs.get_logger(
                    configs[setname].db,
                    configs[setname].dc,
                    "integration-sequences.src",
                ),
            )
            dst_seq_vals = await pgbelt.util.postgres.dump_sequences(
                dst_pool,
                configs[setname].sequences,
                configs[setname].schema_name,
                pgbelt.util.logs.get_logger(
                    configs[setname].db,
                    configs[setname].dc,
                    "integration-sequences.dst",
                ),
            )

            print(
                f"Ensuring {setname} source and destination sequences are the same..."
            )
            assert src_seq_vals == dst_seq_vals
        else:
            print(f"Ensuring {setname} source and destination dumps are the same...")
            assert src_dumps_filtered[i] == dst_dumps_filtered[i]


async def _test_teardown_not_full(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.teardown.teardown(db=None, dc=configs[list(configs.keys())[0]].dc)

    # TODO: test that the appropriate teardown steps were run for a non-full teardown

    await _check_status(configs, "unconfigured", "unconfigured")


async def _test_teardown_full(configs: dict[str, DbupgradeConfig]):
    await pgbelt.cmd.teardown.teardown(
        db=None, dc=configs[list(configs.keys())[0]].dc, full=True
    )

    # TODO: test that the appropriate teardown steps were run for a full teardown

    await _check_status(configs, "unconfigured", "unconfigured")


async def _test_main_workflow(configs: dict[str, DbupgradeConfig]):
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

    await _test_check_connectivity(configs)
    await _test_precheck(configs)
    await _test_setup(configs)
    await _test_setup_back_replication(configs)
    await _test_create_indexes(configs)
    await _test_analyze(configs)
    await _test_revoke_logins(configs)
    await _test_teardown_forward_replication(configs)
    await _test_sync(configs)

    # Check if the data is the same before testing teardown
    await _ensure_same_data(configs)

    await _test_teardown_not_full(configs)
    await _test_teardown_full(configs)


# Run the main integration test.
# 4 sets of DBs are created: public vs non-public schema, and exodus-style vs full migration.
# Use pgbelt's native async parallelization to run the main workflow on the total set of DBs.
@pytest.mark.asyncio
async def test_main_workflow(setup_db_upgrade_configs):

    await _test_main_workflow(setup_db_upgrade_configs)
