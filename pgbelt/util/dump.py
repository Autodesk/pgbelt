import asyncio
from logging import Logger
from os.path import join
from re import search

from aiofiles import open as aopen
from asyncpg import create_pool

from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.asyncfuncs import isfile
from pgbelt.util.asyncfuncs import listdir
from pgbelt.util.asyncfuncs import makedirs
from pgbelt.util.postgres import table_empty

RAW = "schema"
NO_INVALID = "no_invalid_constraints"
ONLY_INVALID = "invalid_constraints"


def schema_dir(db: str, dc: str) -> str:
    return f"schemas/{dc}/{db}"


def schema_file(db: str, dc: str, name: str) -> str:
    return join(schema_dir(db, dc), f"{name}.sql")


def table_dir(db: str, dc: str) -> str:
    return f"tables/{dc}/{db}"


def table_file(db: str, dc: str, name: str) -> str:
    return join(table_dir(db, dc), f"{name}.sql")


def _parse_dump_commands(out: str, src_owner: str, dst_owner: str) -> list[str]:
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

        # replace source owner user with target owner user
        line = line.replace(src_owner, dst_owner)

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
                    f"--table={table}",
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
            if await table_empty(pool, t, logger):
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
    one with only the NOT VALID constraints from the schema, and one with everything
    but the NOT VALID constraints.
    """
    logger.info("Dumping schema...")

    command = [
        "pg_dump",
        "--schema-only",
        "--no-owner",
        "-n",
        "public",
        config.src.pglogical_dsn,
    ]

    out = await _execute_subprocess(command, "Retrieved source schema", logger)

    commands_raw = _parse_dump_commands(
        out.decode("utf-8"), config.src.owner_user.name, config.dst.owner_user.name
    )

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

    async with aopen(schema_file(config.db, config.dc, NO_INVALID), "w") as out:
        for command in commands:
            if "NOT VALID" not in command:
                await out.write(command)

    logger.debug("Finished dumping schema.")


async def apply_target_schema(config: DbupgradeConfig, logger: Logger) -> None:
    """
    Load the schema dumped from the source into the target excluding NOT VALID constraints.
    """
    logger.info("Loading schema without constraints...")

    command = [
        "psql",
        config.dst.owner_dsn,
        "-f",
        schema_file(config.db, config.dc, NO_INVALID),
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
        "public",
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

    logger.info("Removing NOT VALID constraints...")

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

    command = ["psql", config.dst.owner_dsn, "-c", f"'{queries}'"]

    await _execute_subprocess(
        command, "Finished removing NOT VALID constraints.", logger
    )


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
