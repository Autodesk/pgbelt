import asyncio
from logging import Logger
from os.path import join
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.asyncfuncs import isfile
from pgbelt.util.asyncfuncs import listdir
from pgbelt.util.asyncfuncs import makedirs
from pgbelt.util.postgres import table_empty
from re import search

from aiofiles import open as aopen
from asyncpg import create_pool

RAW = "schema"
NO_INVALID_NO_INDEX = "no_invalid_constraints_no_indexes"
ONLY_INVALID = "invalid_constraints"
ONLY_INDEXES = "indexes"


def schema_dir(db: str, dc: str) -> str:
    return f"schemas/{dc}/{db}"


def schema_file(db: str, dc: str, name: str) -> str:
    return join(schema_dir(db, dc), f"{name}.sql")


def table_dir(db: str, dc: str) -> str:
    return f"tables/{dc}/{db}"


def table_file(db: str, dc: str, name: str) -> str:
    return join(table_dir(db, dc), f"{name}.sql")


def _parse_dump_commands(out: str) -> list[str]:
    """
    Given a string containing output from pg_dump, return a list of strings where
    each is a complete postgres command. Commands may be multi-line.
    """
    lines = out.split("\n")
    commands = []

    for line in lines:
        stripped = line.strip()
        # if the line is whitespace only or a comment then ignore it
        if not stripped or stripped.startswith("--"):
            continue

        # if the last command is terminated or we don't have any yet start a new one
        if not commands or commands[-1].endswith(";\n"):
            commands.append(line + "\n")
        # otherwise we append to the last command because it must be multi-line
        else:
            commands[-1] += line + "\n"

    return commands


async def _execute_subprocess(
    command: list[str], finished_log: str, logger: Logger
) -> bytes:
    p = await asyncio.create_subprocess_exec(
        command[0],
        *command[1:],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await p.communicate()

    if p.returncode != 0:
        raise Exception(
            f"Couldn't do {command}, got code {p.returncode}.\n  out: {out.decode('utf-8')}\n  err: {err.decode('utf-8')}"
        )
    logger.debug(finished_log)
    return out


async def dump_source_tables(
    config: DbupgradeConfig, tables: list[str], logger: Logger
) -> None:
    try:
        await makedirs(table_dir(config.db, config.dc))
    except FileExistsError:
        pass

    logger.info(f"Dumping tables {tables}")

    dumps = []
    for table in tables:
        dumps.append(
            _execute_subprocess(
                [
                    "pg_dump",
                    "--data-only",
                    f"--table={config.schema_name}.{table}",
                    "-Fc",
                    "-f",
                    table_file(config.db, config.dc, table),
                    config.src.pglogical_dsn,
                ],
                f"dumped {table}",
                logger,
            )
        )

    await asyncio.gather(*dumps)


async def load_dumped_tables(
    config: DbupgradeConfig, tables: list[str], logger: Logger
) -> None:
    # unless we get an explicit list of tables to load just load all the dump files
    if not tables:
        tables_dir = table_dir(config.db, config.dc)
        tables = [
            f.split(".")[0]
            for f in await listdir(tables_dir)
            if await isfile(join(tables_dir, f))
        ]

    logger.info(f"Loading dumped tables {tables}")

    # only load a dump file if the target table is completely empty
    async with create_pool(config.dst.root_uri, min_size=1) as pool:
        to_load = []
        for t in tables:
            if await table_empty(pool, t, config.schema_name, logger):
                to_load.append(table_file(config.db, config.dc, t))
            else:
                logger.warning(
                    f"Not loading {t}, table not empty. If this is unexpected please investigate."
                )

    loads = []
    for file in to_load:
        loads.append(
            _execute_subprocess(
                [
                    "pg_restore",
                    "-d",
                    config.dst.owner_dsn,
                    file,
                ],
                f"loaded {file}",
                logger,
            )
        )

    await asyncio.gather(*loads)


async def dump_source_schema(config: DbupgradeConfig, logger: Logger) -> None:
    """
    Dump the schema from the source db and write a file with the complete schema,
    one with only the CREATE INDEX statements from the schema,
    one with only the NOT VALID constraints from the schema,
    and one with everything but the NOT VALID constraints and the CREATE INDEX statements.
    """
    logger.info("Dumping schema...")

    command = [
        "pg_dump",
        "--schema-only",
        "--no-owner",
        "-n",
        config.schema_name,
        config.src.pglogical_dsn,
    ]

    # TODO: We should exclude the creation of a schema in the schema dump and load, and made that the responsibility of the user.
    # Confirm if the CREATE SCHEMA statement is included in the schema dump, and if yes, exclude it.
    # This will reveal itself in the integration test.

    out = await _execute_subprocess(command, "Retrieved source schema", logger)

    commands_raw = _parse_dump_commands(out.decode("utf-8"))

    commands = []
    for c in commands_raw:
        if "EXTENSION " not in c and "GRANT " not in c and "REVOKE " not in c:
            commands.append(c)

    try:
        await makedirs(schema_dir(config.db, config.dc))
    except FileExistsError:
        pass

    async with aopen(schema_file(config.db, config.dc, RAW), "w") as out:
        for command in commands:
            await out.write(command)

    async with aopen(schema_file(config.db, config.dc, ONLY_INVALID), "w") as out:
        for command in commands:
            if "NOT VALID" in command:
                await out.write(command)

    async with aopen(schema_file(config.db, config.dc, ONLY_INDEXES), "w") as out:
        for command in commands:
            if "CREATE" in command and "INDEX" in command:
                await out.write(command)

    async with aopen(
        schema_file(config.db, config.dc, NO_INVALID_NO_INDEX), "w"
    ) as out:
        for command in commands:
            if not ("NOT VALID" in command) and not (
                "CREATE" in command and "INDEX" in command
            ):
                await out.write(command)

    logger.debug("Finished dumping schema.")


async def apply_target_schema(config: DbupgradeConfig, logger: Logger) -> None:
    """
    Load the schema dumped from the source into the target excluding NOT VALID constraints and CREATE INDEX statements.
    """
    logger.info("Loading schema without constraints...")

    command = [
        "psql",
        config.dst.owner_dsn,
        "-f",
        schema_file(config.db, config.dc, NO_INVALID_NO_INDEX),
    ]

    await _execute_subprocess(command, "Finished loading schema.", logger)


async def dump_dst_not_valid_constraints(
    config: DbupgradeConfig, logger: Logger
) -> None:
    """
    Dump NOT VALID Constraints from the target database.
    Used when schema is loaded in outside of pgbelt.
    """

    logger.info("Dumping target NOT VALID constraints...")

    command = [
        "pg_dump",
        "--schema-only",
        "--no-owner",
        "-n",
        config.schema_name,
        config.dst.pglogical_dsn,
    ]

    out = await _execute_subprocess(command, "Retrieved target schema", logger)

    # No username replacement needs to be done, so replace dst user with the same.
    commands_raw = _parse_dump_commands(
        out.decode("utf-8"), config.dst.owner_user.name, config.dst.owner_user.name
    )

    commands = []
    for c in commands_raw:
        if "NOT VALID" in command:
            if config.tables:
                regex_matches = search(
                    r"ALTER TABLE [ONLY ]*(?P<table>[a-zA-Z0-9._]+)+\s+ADD CONSTRAINT (?P<constraint>[a-zA-Z0-9._]+)+.*",
                    c,
                )
                if not regex_matches:
                    continue
                table = regex_matches.groupdict()["table"]
                if config.tables and table in config.tables:
                    commands.append(c)
            else:
                commands.append(c)

    try:
        await makedirs(schema_dir(config.db, config.dc))
    except FileExistsError:
        pass

    async with aopen(schema_file(config.db, config.dc, ONLY_INVALID), "w") as out:
        for command in commands:
            await out.write(command)

    logger.debug("Finished dumping NOT VALID constraints from the target.")


async def remove_dst_not_valid_constraints(
    config: DbupgradeConfig, logger: Logger
) -> None:
    """
    Remove the NOT VALID constraints from the schema of the target database.
    Only use if target schema was loaded in without pgbelt.
    """
    logger.info("Looking for previously dumped NOT VALID constraints...")

    async with aopen(schema_file(config.db, config.dc, ONLY_INVALID), "r") as f:
        not_valid_constraints = await f.read()

    logger.info("Removing NOT VALID constraints from the target...")

    queries = ""
    for c in not_valid_constraints.split(";"):
        regex_matches = search(
            r"ALTER TABLE [ONLY ]*(?P<table>[a-zA-Z0-9._]+)+\s+ADD CONSTRAINT (?P<constraint>[a-zA-Z0-9._]+)+.*",
            c,
        )
        if not regex_matches:
            continue
        table = regex_matches.groupdict()["table"]
        constraint = table = regex_matches.groupdict()["constraint"]

        if (config.tables and table in config.tables) or not config.tables:
            queries = queries + f"ALTER TABLE {table} DROP CONSTRAINT {constraint};"

    if queries != "":
        command = ["psql", config.dst.owner_dsn, "-c", f"'{queries}'"]

        await _execute_subprocess(
            command, "Finished removing NOT VALID constraints from the target.", logger
        )
    else:
        logger.info("No NOT VALID detected for removal.")


async def apply_target_constraints(config: DbupgradeConfig, logger: Logger) -> None:
    """
    Load the NOT VALID constraints that were excluded from the schema. Should be called after replication during
    downtime before allowing writes into the target.
    """
    logger.info("Loading NOT VALID constraints...")

    command = [
        "psql",
        config.dst.owner_dsn,
        "-f",
        schema_file(config.db, config.dc, ONLY_INVALID),
    ]

    await _execute_subprocess(
        command, "Finished loading NOT VALID constraints.", logger
    )


async def remove_dst_indexes(config: DbupgradeConfig, logger: Logger) -> None:
    """
    Remove the INDEXes from the schema of the target database.
    Only use if target schema was loaded in without pgbelt.
    """
    logger.info("Looking for previously dumped CREATE INDEX statements...")

    async with aopen(schema_file(config.db, config.dc, ONLY_INDEXES), "r") as f:
        create_index_statements = await f.read()

    logger.info("Removing Indexes from the target...")

    for c in create_index_statements.split(";"):
        regex_matches = search(
            r"CREATE [UNIQUE ]*INDEX (?P<index>[a-zA-Z0-9._]+)+.*",
            c,
        )
        if not regex_matches:
            continue
        index = regex_matches.groupdict()["index"]
        if config.schema_name:
            index = f"{config.schema_name}.{index}"

        # DROP the index
        # Note that the host DSN must have a statement timeout of 0.
        # Example DSN: `host=server-hostname user=user dbname=db_name options='-c statement_timeout=3600000'`
        host_dsn = config.dst.owner_dsn + " options='-c statement_timeout=0'"

        # DROP INDEX IF EXISTS so no need to catch exceptions
        command = ["psql", host_dsn, "-c", f"DROP INDEX IF EXISTS {index};"]
        logger.info(f"Dropping index {index} on the target...")
        await _execute_subprocess(
            command, f"Finished dropping index {index} on the target.", logger
        )


async def create_target_indexes(
    config: DbupgradeConfig, logger: Logger, during_sync=False
) -> None:
    """
    Create indexes on the target that were excluded from the schema during setup.
    Should be called once bulk syncing is complete, and before cutover.

    Runs in serial for now with this async code.
    TODO: make this run in parallel (beware risk of building too many indexes at once, resource heavy)
    """

    if during_sync:
        logger.warning(
            "Attempting to create indexes on the target. If indexes were not created before the cutover window, this can take a long time."
        )

    logger.info("Looking for previously dumped CREATE INDEX statements...")

    async with aopen(schema_file(config.db, config.dc, ONLY_INDEXES), "r") as f:
        create_index_statements = await f.read()

    logger.info("Creating indexes on the target...")

    for c in create_index_statements.split(";"):
        # Get the Index Name
        regex_matches = search(
            r"CREATE [UNIQUE ]*INDEX (?P<index>[a-zA-Z0-9._]+)+.*",
            c,
        )
        if not regex_matches:
            continue
        index = regex_matches.groupdict()["index"]

        # Create the index
        # Note that the host DSN must have a statement timeout of 0.
        # Example DSN: `host=server-hostname user=user dbname=db_name options='-c statement_timeout=3600000'`
        host_dsn = config.dst.owner_dsn + " options='-c statement_timeout=0'"
        command = ["psql", host_dsn, "-c", f"{c};"]
        logger.info(f"Creating index {index} on the target...")
        try:
            await _execute_subprocess(
                command, f"Finished creating index {index} on the target.", logger
            )
        except Exception as e:
            if f'relation "{index}" already exists' in str(e):
                logger.info(f"Index {index} already exist on the target.")
            else:
                raise Exception(e)
