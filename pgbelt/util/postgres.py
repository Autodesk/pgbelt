from logging import Logger

from decimal import Decimal
from asyncpg import Pool
from asyncpg import Record
from asyncpg.exceptions import UndefinedObjectError


async def dump_sequences(
    pool: Pool, targeted_sequences: list[str], schema: str, logger: Logger
) -> dict[str, int]:
    """
    return a dictionary of sequence names mapped to their last values
    """
    logger.info("Dumping sequence values...")
    # Get all sequences in the schema
    seqs = await pool.fetch(
        f"""
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = '{schema}';
        """
    )

    # Note, in exodus migrations, we expect the sequence names to not contain the schema name when coming into targeted_sequences.

    seq_vals = {}
    final_seqs = []
    # If we get a list of targeted sequences, we only want to dump whichever of those are found in the database and schema.
    if targeted_sequences:
        final_seqs = [r[0] for r in seqs if r[0] in targeted_sequences]
    else:  # Otherwise, we want to dump all sequences found in the schema.
        final_seqs = [r[0] for r in seqs]

    for seq in final_seqs:
        res = await pool.fetchval(f'SELECT last_value FROM {schema}."{seq}";')
        seq_vals[seq.strip()] = res

    logger.debug(f"Dumped sequences: {seq_vals}")
    return seq_vals


async def load_sequences(
    pool: Pool, seqs: dict[str, int], schema: str, logger: Logger
) -> None:
    """
    given a dict of sequence named mapped to values, set each sequence to the
    matching value
    """

    # If seqs is empty, we have nothing to do. Skip the operation.
    if not seqs:
        logger.info("No sequences to load. Skipping sequence loading.")
        return

    logger.info(f"Loading sequences {list(seqs.keys())} from schema {schema}...")
    sql_template = "SELECT pg_catalog.setval('{}.\"{}\"', {}, true);"
    sql = "\n".join([sql_template.format(schema, k, v) for k, v in seqs.items()])
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(sql)

    logger.debug("Loaded sequences")


async def compare_data(
    src_pool: Pool,
    dst_pool: Pool,
    query: str,
    tables: list[str],
    schema: str,
    logger: Logger,
) -> None:
    """
    Validate data between source and destination databases by doing the following:
    1. Get all tables with primary keys (from the source)
    2. For each of those tables, select * limit 100
    3. For each row, ensure the row in the destination is identical
    """
    pkeys, _, pkeys_raw = await analyze_table_pkeys(src_pool, schema, logger)

    pkeys_dict = {}
    # {
    #     "table1": ["pkey1", "pkey2", ...],
    #     ...
    # }
    for row in pkeys_raw:
        if row[0] in pkeys_dict:
            pkeys_dict[row[0]].append(row[3])
        else:
            pkeys_dict[row[0]] = [row[3]]

    src_old_extra_float_digits = await src_pool.fetchval("SHOW extra_float_digits;")
    await src_pool.execute("SET extra_float_digits TO 0;")

    dst_old_extra_float_digits = await dst_pool.fetchval("SHOW extra_float_digits;")
    await dst_pool.execute("SET extra_float_digits TO 0;")

    has_run = False
    for table in set(pkeys):
        # If specific table list is defined and the iterated table is not in that list, skip.
        if tables and (table not in tables):
            continue

        has_run = True  # If this runs, we have at least one table to compare. We will use this flag to throw an error if no tables are found.

        full_table_name = f'{schema}."{table}"'

        logger.debug(f"Validating table {full_table_name}...")

        # Have to wrap each pkey in double quotes due to capitalization issues.
        order_by_pkeys = ""
        for pkey in pkeys_dict[table]:
            order_by_pkeys += f'"{pkey}", '
        order_by_pkeys = order_by_pkeys[:-2]

        filled_query = query.format(
            table=full_table_name, order_by_pkeys=order_by_pkeys
        )

        src_rows = await src_pool.fetch(filled_query)

        # There is a chance tables are empty...
        if len(src_rows) == 0:
            dst_rows = await dst_pool.fetch(filled_query)
            if len(dst_rows) != 0:
                raise AssertionError(
                    f"Table {full_table_name} has 0 rows in source but nonzero rows in target... Big problem. Please investigate."
                )
            else:
                continue

        pkey_vals_dict = {}
        # {
        #     "pkey1": "1,2,3,4,5,6..."
        #     "pkey2": "'a','b','c'..."
        #     ...
        # }
        for pkey in pkeys_dict[table]:
            src_pkeys_string = ""
            for row in src_rows:
                pkey_val = row[pkey]
                if isinstance(pkey_val, int):
                    src_pkeys_string += f"{pkey_val},"
                else:
                    src_pkeys_string += f"'{pkey_val}',"
            src_pkeys_string = src_pkeys_string[:-1]
            pkey_vals_dict[pkey] = src_pkeys_string

        dst_query = f"SELECT * FROM {full_table_name} WHERE "

        for k, v in pkey_vals_dict.items():
            dst_query = dst_query + f'"{k}" IN ({v}) AND '

        # SELECT * FROM <table> WHERE <pkey1> IN (1,2,3,4,5,6...) AND <pkey2> IN ('a','b','c',...);
        comparison_query = dst_query[:-5] + f" ORDER BY {order_by_pkeys};"

        # This is pretty wild. So in the first query, if you have a compounding key (>1 PK)
        # and you limit with a number, the entropy of your keys can exceed the limit in reality.
        # So since we gathered the PKs, we should run the query again on the source to get the full
        # dataset then compare. Otherwise, this code will fail citing the row count is not equal.
        src_rows = await src_pool.fetch(comparison_query)
        dst_rows = await dst_pool.fetch(comparison_query)

        if len(src_rows) != len(dst_rows):
            raise AssertionError(
                f'Row count of the sample taken from table "{full_table_name}" '
                "does not match in source and destination!\n"
                f"Query: {dst_query}"
            )

        # Check each row for exact match
        for src_row, dst_row in zip(src_rows, dst_rows):
            if src_row != dst_row:

                # Addresses #571, AsyncPG is decoding numeric NaN as Python Decimal('NaN').
                # Decimal('NaN') != Decimal('NaN'), breaks comparison. Convert those NaNs to None.
                src_row_d = {
                    key: (
                        value
                        if not (isinstance(value, Decimal) and value.is_nan())
                        else None
                    )
                    for key, value in row.items()
                }
                dst_row_d = {
                    key: (
                        value
                        if not (isinstance(value, Decimal) and value.is_nan())
                        else None
                    )
                    for key, value in row.items()
                }

                if src_row_d != dst_row_d:
                    raise AssertionError(
                        "Row match failure between source and destination.\n"
                        f"Table: {full_table_name}\n"
                        f"Source Row: {src_row}\n"
                        f"Dest Row: {dst_row}"
                    )

    # Just a paranoia check. If this throws, then it's possible pgbelt didn't migrate any data.
    # This was found in issue #420, and previous commands threw errors before this issue could arise.
    if not has_run:
        raise ValueError(
            "No tables were found to compare. Please reach out to the pgbelt for help, and check if your data was migrated."
        )

    await src_pool.execute(f"SET extra_float_digits TO {src_old_extra_float_digits};")
    await dst_pool.execute(f"SET extra_float_digits TO {dst_old_extra_float_digits};")
    logger.info(
        "Validation Complete. Samples match in both Source and Destination Databases!"
    )


async def compare_100_rows(
    src_pool: Pool, dst_pool: Pool, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Validate data between source and destination databases by doing the following:
    1. Get all tables with primary keys
    2. For each of those tables, select * limit 100
    3. For each row, ensure the row in the destination is identical
    """
    logger.info("Comparing 100 rows...")

    query = """
    SELECT * FROM
    (
        SELECT *
        FROM {table}
        LIMIT 100
    ) AS T1
    ORDER BY {order_by_pkeys};
    """

    await compare_data(src_pool, dst_pool, query, tables, schema, logger)


async def compare_latest_100_rows(
    src_pool: Pool, dst_pool: Pool, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Validate data between source and destination databases by comparing the latest row:
    1. Get all tables with primary keys
    2. For each of those tables, select * limit 1 order by PK DESC
    3. For each row, ensure the row in the destination is identical
    """
    logger.info("Comparing latest 100 rows...")

    query = """
    SELECT *
    FROM {table}
    ORDER BY {order_by_pkeys} DESC
    LIMIT 100;
    """

    await compare_data(src_pool, dst_pool, query, tables, schema, logger)


async def table_empty(pool: Pool, table: str, schema: str, logger: Logger) -> bool:
    """
    return true if the table is empty
    """
    logger.info(f"Checking if table {table} is empty...")
    result = await pool.fetch(f"SELECT * FROM {schema}.{table} LIMIT 1;")
    return len(result) == 0


async def analyze_table_pkeys(
    pool: Pool, schema: str, logger: Logger
) -> tuple[list[str], list[str], Record]:
    """
    Return three lists of table names. the first element is all tables
    with pkeys in the config's named schema and the second is all tables
    without pkeys in that schema. The third list is the raw rows of the
    primary key query with the table name, constraint name, position and
    column name for the primary key.
    """
    logger.info("Checking table primary keys...")
    pkeys_raw = await pool.fetch(
        f"""
        SELECT kcu.table_name,
            tco.constraint_name,
            kcu.ordinal_position as position,
            kcu.column_name as key_column
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tco.constraint_name
            AND kcu.constraint_schema = tco.constraint_schema
            AND kcu.constraint_name = tco.constraint_name
        WHERE tco.constraint_type = 'PRIMARY KEY'
            AND kcu.table_schema = '{schema}'
        ORDER BY kcu.table_name,
                position;
        """
    )
    pkeys = [r[0] for r in pkeys_raw]

    all_tables = await pool.fetch(
        f"""SELECT table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = '{schema}'
            AND table_name != 'pg_stat_statements'
        ORDER BY 1;"""
    )
    no_pkeys = [r[0] for r in all_tables if r[0] not in pkeys]

    return pkeys, no_pkeys, pkeys_raw


async def run_analyze(pool: Pool, logger: Logger) -> None:
    """
    Run ANALYZE
    """
    logger.info("running ANALYZE...")
    await pool.execute("ANALYZE;")
    logger.debug("ANALYZE completed.")


async def get_login_users(pool: Pool, logger: Logger) -> list[str]:
    """
    Returns a list of all the users who can log in.
    """
    logger.debug("Finding users who can log in...")
    user_rows = await pool.fetch(
        "SELECT rolname FROM pg_catalog.pg_roles WHERE rolcanlogin;"
    )
    usernames = [r[0] for r in user_rows]
    return usernames


async def disable_login_users(pool: Pool, users: list[str], logger: Logger) -> None:
    """
    Revokes login permissions from all users in the given list.
    """
    logger.info(f"Disabling login for users: {users}")
    async with pool.acquire() as conn:
        async with conn.transaction():
            for user in users:
                await conn.execute(f'ALTER ROLE "{user}" WITH NOLOGIN;')


async def enable_login_users(pool: Pool, users: list[str], logger: Logger) -> None:
    """
    Restores login permissions for all users in the given list.
    """
    logger.info(f"Enabling login for users {users}")
    async with pool.acquire() as conn:
        async with conn.transaction():
            for user in users:
                await conn.execute(f'ALTER ROLE "{user}" WITH LOGIN;')


async def precheck_info(
    pool: Pool,
    root_name: str,
    owner_name: str,
    target_tables: list[str],
    target_sequences: list[str],
    schema: str,
    logger: Logger,
) -> dict:
    """
    Return a dictionary of information about the database used to determine
    whether belt will work on it and what can be migrated.
    """
    logger.info("Checking db requirements...")
    result = {
        "server_version": await pool.fetchval("SHOW server_version"),
        "max_replication_slots": await pool.fetchval("SHOW max_replication_slots;"),
        "max_worker_processes": await pool.fetchval("SHOW max_worker_processes;"),
        "max_wal_senders": await pool.fetchval("SHOW max_wal_senders;"),
        "shared_preload_libraries": await pool.fetchval(
            "SHOW shared_preload_libraries;"
        ),
        "tables": [],
        "sequences": [],
        "users": {},
        "extensions": [],
    }

    # server_version shows 13.14 (Debian 13.14-1.pgdg120+2) in the output. Remove the Debian part.
    result["server_version"] = result["server_version"].split(" ")[0]

    try:
        result["rds.logical_replication"] = await pool.fetchval(
            "SHOW rds.logical_replication;"
        )
    except UndefinedObjectError:
        result["rds.logical_replication"] = "Not Applicable"

    result["tables"] = await pool.fetch(
        """
        SELECT n.nspname as "Schema",
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','p','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
              AND n.nspname <> 'pglogical'
        ORDER BY 1,2;"""
    )

    # We filter the table list if the user has specified a list of tables to target.
    if target_tables:

        result["tables"] = [t for t in result["tables"] if t["Name"] in target_tables]

        # We will not recapitalize the table names in the result["tables"] list,
        # to preserve how Postgres sees those tables in its system catalog. Easy
        # rabbit hole later if we keep patching the table names to match the user's
        # input.

    result["sequences"] = await pool.fetch(
        """
        SELECT n.nspname as "Schema",
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('S','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
              AND n.nspname <> 'pglogical'
        ORDER BY 1,2;"""
    )

    # We filter the table list if the user has specified a list of tables to target.
    if target_sequences:

        result["sequences"] = [
            t for t in result["sequences"] if t["Name"] in target_sequences
        ]

        # We will not recapitalize the table names in the result["tables"] list,
        # to preserve how Postgres sees those tables in its system catalog. Easy
        # rabbit hole later if we keep patching the table names to match the user's
        # input.

    users = await pool.fetch(
        f"""
        SELECT r.rolname, r.rolsuper, r.rolinherit,
          r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
          r.rolconnlimit, r.rolvaliduntil,
          ARRAY(SELECT b.rolname
                FROM pg_catalog.pg_auth_members m
                JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
                WHERE m.member = r.oid) as memberof
        , r.rolreplication
        , r.rolbypassrls
        , has_schema_privilege(r.rolname, '{schema}', 'CREATE') AS can_create
        FROM pg_catalog.pg_roles r
        WHERE r.rolname !~ '^pg_' AND (r.rolname = '{root_name}' OR r.rolname = '{owner_name}')
        ORDER BY 1;"""
    )

    # We only care about the root and owner users.
    for u in users:
        if u[0] == root_name:
            result["users"]["root"] = u
        if u[0] == owner_name:
            result["users"]["owner"] = u

    result["extensions"] = await pool.fetch(
        """
        SELECT extname
        FROM pg_extension
        ORDER BY extname;
        """
    )

    return result


# TODO: Need to add schema here when working on non-public schema support.
async def get_dataset_size(
    tables: list[str], schema: str, pool: Pool, logger: Logger
) -> str:
    """
    Get the total disk size of a dataset (via list of tables).

    This function ALWAYS expects a list of tables. If not, the calling function should handle that.
    """
    logger.info("Getting the targeted dataset size...")

    # Tables string must be of form "'table1', 'table2', ..."
    tables_string = ", ".join([f"'{t}'" for t in tables])

    query = f"""
    SELECT
        sum(pg_total_relation_size(schemaname || '."' || tablename || '"')) AS total_relation_size
    FROM
        pg_tables
    WHERE
        schemaname = '{schema}'
    AND tablename IN ({tables_string});
    """

    # Yes it's a duplicate, but it's a pretty one. Rather let Postgres do this than Python.
    pretty_query = f"""
    SELECT
        pg_size_pretty(sum(pg_total_relation_size(schemaname || '."' || tablename || '"'))) AS total_relation_size
    FROM
        pg_tables
    WHERE
        schemaname = '{schema}'
    AND tablename IN ({tables_string});
    """

    result = {
        "db_size": await pool.fetchval(query),
        "db_size_pretty": await pool.fetchval(pretty_query),
    }

    return result


async def initialization_progress(
    tables: list[str],
    src_schema: str,
    dst_schema: str,
    src_pool: Pool,
    dst_pool: Pool,
    src_logger: Logger,
    dst_logger: Logger,
) -> dict[str, str]:
    """
    Get the size progress of the initialization stage
    """

    src_dataset_size = await get_dataset_size(tables, src_schema, src_pool, src_logger)
    dst_dataset_size = await get_dataset_size(tables, dst_schema, dst_pool, dst_logger)

    # Eliminate None values
    if src_dataset_size["db_size"] is None:
        src_dataset_size["db_size"] = 0
    if dst_dataset_size["db_size"] is None:
        dst_dataset_size["db_size"] = 0

    # Eliminate division by zero
    if src_dataset_size["db_size"] == 0 and dst_dataset_size["db_size"] == 0:
        progress = "0 %"
    else:
        progress = f"{str(round(int(dst_dataset_size['db_size'])/int(src_dataset_size['db_size'])*100 ,1))} %"

    status = {
        "src_dataset_size": src_dataset_size["db_size_pretty"] or "0 bytes",
        "dst_dataset_size": dst_dataset_size["db_size_pretty"] or "0 bytes",
        "progress": progress,
    }
    return status
