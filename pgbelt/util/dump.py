import asyncio
from logging import Logger
from os.path import join
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.asyncfuncs import makedirs
from pgbelt.util.postgres import table_empty
from re import finditer, IGNORECASE, search

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


def _parse_dump_commands(out: str) -> list[str]:
    """
    Given a string containing output from pg_dump, return a list of strings where
    each is a complete postgres command. Commands may be multi-line.

    Dollar-quoted strings (e.g. function bodies between $_$ ... $_$) are treated
    as opaque content — semicolons within them do not affect command boundary
    detection.
    """
    lines = out.split("\n")
    commands = []
    in_dollar_quote = False
    dollar_quote_tag = None

    for line in lines:
        stripped = line.strip()
        # if the line is whitespace only or a comment then ignore it
        if not stripped or stripped.startswith("--"):
            continue

        # Start a new command if we have no commands yet, or the previous command
        # is fully terminated (ends with ;) and we're not inside a dollar-quoted string.
        if not commands or (commands[-1].endswith(";\n") and not in_dollar_quote):
            commands.append(line + "\n")
        else:
            commands[-1] += line + "\n"

        # Track dollar-quoting by scanning for dollar-quote tags in this line.
        # A dollar-quote tag is $$ or $tag$ where tag matches [a-zA-Z_][a-zA-Z0-9_]*.
        for match in finditer(r"\$([a-zA-Z_][a-zA-Z0-9_]*)?\$", line):
            tag = match.group(0)
            if not in_dollar_quote:
                in_dollar_quote = True
                dollar_quote_tag = tag
            elif tag == dollar_quote_tag:
                in_dollar_quote = False
                dollar_quote_tag = None

    return commands


def _normalize_columns(cols_str: str) -> tuple:
    """
    Normalize a comma-separated column list for set comparison.
    Strips whitespace, removes quotes, lowercases, and sorts alphabetically.
    """
    cols = [c.strip().replace('"', "").lower() for c in cols_str.split(",")]
    return tuple(sorted(cols))


def _find_fk_required_unique_indexes(commands: list[str], logger: Logger) -> set[int]:
    """
    Given a list of schema commands from pg_dump, identify CREATE UNIQUE INDEX
    commands that are required by FOREIGN KEY constraints.

    A FK constraint like: FOREIGN KEY (col) REFERENCES parent_table(ref_col)
    requires that parent_table has a unique constraint on ref_col. If the only
    such constraint is a CREATE UNIQUE INDEX (rather than a PRIMARY KEY or
    ALTER TABLE ADD CONSTRAINT ... UNIQUE), the index must be present before
    the FK can be applied.

    This function finds matching unique indexes and returns their indices in the
    commands list so they can be included in the base schema rather than being
    deferred.

    Partial unique indexes (those with a WHERE clause) are excluded because
    PostgreSQL does not accept them as FK targets.
    """

    # Collect (table, columns) tuples referenced by FK constraints
    fk_references = set()
    for command in commands:
        if "FOREIGN KEY" in command and "REFERENCES" in command:
            match = search(
                r"REFERENCES\s+(?P<ref_table>[^\s(]+)\s*\((?P<ref_cols>[^)]+)\)",
                command,
            )
            if match:
                ref_table = match.group("ref_table").replace('"', "").lower()
                ref_cols = _normalize_columns(match.group("ref_cols"))
                fk_references.add((ref_table, ref_cols))

    if not fk_references:
        return set()

    # Find CREATE UNIQUE INDEX commands whose table(columns) match an FK reference
    fk_required = set()
    for i, command in enumerate(commands):
        if "CREATE" in command and "UNIQUE" in command and "INDEX" in command:
            # Partial unique indexes (with WHERE clause) cannot satisfy FKs
            if search(r"\)\s*WHERE\s+", command, IGNORECASE):
                continue
            match = search(
                r"CREATE\s+UNIQUE\s+INDEX\s+[^\s]+\s+ON\s+(?P<table>[^\s(]+)"
                r"\s+(?:USING\s+\w+\s+)?\((?P<cols>[^)]+)\)",
                command,
            )
            if match:
                table = match.group("table").replace('"', "").lower()
                cols = _normalize_columns(match.group("cols"))
                if (table, cols) in fk_references:
                    fk_required.add(i)
                    logger.info(
                        f"Unique index on {table}({', '.join(cols)}) "
                        f"is required by a FK constraint and will be "
                        f"included in the base schema."
                    )

    return fk_required


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


async def _pipe_dump_and_load_table(
    config: DbupgradeConfig, table: str, logger: Logger
) -> None:
    """
    Dump a single table from the source and pipe it directly into the
    destination database via a shell pipeline:

        pg_dump | sed (filter) | psql (load in transaction with replica role)

    No intermediate files or in-memory buffers are used. The OS handles
    backpressure between the processes natively.

    The psql side wraps the load in a transaction with
    session_replication_role = replica so triggers don't fire during the load.
    """
    # sed filter: strip unwanted SET commands from pg_dump header.
    # These are not appropriate for the destination (e.g. transaction_timeout
    # may not exist on older PG, search_path should not be overridden).
    sed_filter = (
        "transaction_timeout"
        "|SET client_encoding"
        "|SET standard_conforming_strings"
        "|SET check_function_bodies"
        "|SET xmloption"
        "|SET client_min_messages"
        "|SET row_security"
        "|pg_catalog.set_config"
    )

    table_arg = f'{config.schema_name}."{table}"'

    cmd = (
        f'pg_dump --data-only --table={table_arg} "{config.src.pglogical_dsn}"'
        f" | sed -E '/{sed_filter}/d'"
        f' | psql "{config.dst.root_dsn}" -v ON_ERROR_STOP=1'
        f" -c 'BEGIN; SET LOCAL session_replication_role = replica;'"
        f" -f -"
        f" -c 'COMMIT;'"
    )

    p = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        f"set -o pipefail; {cmd}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await p.communicate()

    if p.returncode != 0:
        raise Exception(
            f"Pipe dump and load of table '{table}' failed with code {p.returncode}.\n"
            f"  out: {out.decode('utf-8')}\n  err: {err.decode('utf-8')}"
        )

    logger.info(f"Piped dump and load of {table} complete.")


async def dump_and_load_tables(
    config: DbupgradeConfig, tables: list[str], logger: Logger
) -> None:
    """
    Dump tables from the source and pipe them directly into the destination.
    Each table is piped independently (pg_dump | psql) with no intermediate
    files or in-memory buffers.

    Only loads into tables that are currently empty on the destination.
    """
    # Check which destination tables are empty before loading
    async with create_pool(config.dst.root_uri, min_size=1) as pool:
        to_load = []
        for t in tables:
            if await table_empty(pool, t, config.schema_name, logger):
                to_load.append(t)
            else:
                logger.warning(
                    f"Not loading {t}, table not empty. "
                    f"If this is unexpected please investigate."
                )

    logger.info(f"Piping dump and load for tables {to_load}")

    loads = [_pipe_dump_and_load_table(config, t, logger) for t in to_load]
    await asyncio.gather(*loads)


async def _dump_and_filter_schema(
    dsn: str, schema_name: str, logger: Logger, full: bool = False
) -> str:
    """
    Run pg_dump -s piped through shell grep filters to produce a clean schema.
    """
    excludes = "EXTENSION |GRANT |REVOKE |\\\\restrict |\\\\unrestrict "
    cmd = (
        f"pg_dump -s --no-owner -n {schema_name} '{dsn}'"
        " | grep -vE '^\\s*$'"
        " | grep -vE '^\\s*--'"
        f" | grep -vE '{excludes}'"
    )
    if not full:
        # Use awk to buffer multi-line commands (accumulate lines until ;)
        # and only print the command if it doesn't contain excluded keywords.
        # This avoids orphaned lines from multi-line NOT VALID or CREATE INDEX statements.
        cmd += (
            " | awk '"
            '/;[[:space:]]*$/ { buf = buf "\\n" $0;'
            " if (buf !~ /NOT VALID/ && buf !~ /CREATE (UNIQUE )?INDEX/) print buf;"
            ' buf = ""; next }'
            ' { buf = (buf == "" ? $0 : buf "\\n" $0) }'
            "'"
        )
    cmd += " | cat -s"
    p = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await p.communicate()
    if p.returncode != 0:
        raise Exception(
            f"Schema dump failed, got code {p.returncode}.\n  err: {err.decode('utf-8')}"
        )
    logger.debug("Retrieved and filtered schema dump.")
    return out.decode("utf-8")


async def validate_schema_dump(
    config: DbupgradeConfig, logger: Logger, full: bool = False
) -> dict:
    """
    Compare the source and destination database schemas by running pg_dump -s
    on both sides, filtered through shell grep pipelines.

    Skips databases with a table list configured (subset migrations).

    Returns a dict with 'db' and 'result' keys.
    """
    if config.tables:
        logger.info("Skipping schema diff: table list configured (subset migration).")
        return {"db": config.db, "result": "skipped"}

    src_filtered, dst_filtered = await asyncio.gather(
        _dump_and_filter_schema(
            config.src.pglogical_dsn, config.schema_name, logger, full
        ),
        _dump_and_filter_schema(
            config.dst.pglogical_dsn, config.schema_name, logger, full
        ),
    )

    if src_filtered == dst_filtered:
        logger.info("Schema diff passed: source and destination match.")
        return {"db": config.db, "result": "match"}
    else:
        from difflib import unified_diff

        diff = "".join(
            unified_diff(
                src_filtered.splitlines(keepends=True),
                dst_filtered.splitlines(keepends=True),
                fromfile="source",
                tofile="destination",
            )
        )
        logger.warning(
            f"Schema diff FAILED: source and destination schemas differ.\n{diff}"
        )
        return {"db": config.db, "result": "mismatch"}


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

    # Identify unique indexes that are required by FK constraints so they
    # can be loaded with the base schema instead of being deferred.
    fk_required_indexes = _find_fk_required_unique_indexes(commands, logger)

    async with aopen(schema_file(config.db, config.dc, ONLY_INDEXES), "w") as out:
        for i, command in enumerate(commands):
            is_index = "CREATE" in command and "INDEX" in command
            if is_index and i not in fk_required_indexes:
                await out.write(command)

    async with aopen(
        schema_file(config.db, config.dc, NO_INVALID_NO_INDEX), "w"
    ) as out:
        for i, command in enumerate(commands):
            is_index = "CREATE" in command and "INDEX" in command
            is_not_valid = "NOT VALID" in command
            if i in fk_required_indexes:
                # FK-required unique indexes go into the base schema
                await out.write(command)
            elif not is_not_valid and not is_index:
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
        constraint = regex_matches.groupdict()["constraint"]

        if (config.tables and table in config.tables) or not config.tables:
            queries = queries + f"ALTER TABLE {table} DROP CONSTRAINT {constraint};"

    if queries != "":
        command = ["psql", config.dst.owner_dsn, "-c", queries]

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
            r"CREATE [UNIQUE ]*INDEX (?P<index>[a-zA-Z0-9._\"]+)+.*",
            c,
        )
        if not regex_matches:
            continue
        index = regex_matches.groupdict()["index"]

        # Sometimes the index name is quoted, so remove the quotes
        index = index.replace('"', "")

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
