from logging import Logger
from typing import Tuple

from asyncpg import Pool
from asyncpg import Record
from asyncpg.exceptions import UndefinedObjectError


async def dump_sequences(
    pool: Pool, targeted_sequences: list[str], logger: Logger
) -> dict[str, int]:
    """
    return a dictionary of sequence names mapped to their last values
    """
    logger.info("Dumping sequence values...")
    seqs = await pool.fetch("SELECT sequence_name FROM information_schema.sequences;")

    seq_vals = {}
    if targeted_sequences:
        for seq in [r[0] for r in seqs if r[0] in targeted_sequences]:
            seq_vals[seq.strip()] = await pool.fetchval(
                f"SELECT last_value FROM {seq};"
            )
    else:
        for seq in [r[0] for r in seqs]:
            seq_vals[seq.strip()] = await pool.fetchval(
                f"SELECT last_value FROM {seq};"
            )

    logger.debug(f"Dumped sequences: {seq_vals}")
    return seq_vals


async def load_sequences(pool: Pool, seqs: dict[str, int], logger: Logger) -> None:
    """
    given a dict of sequence named mapped to values, set each sequence to the
    matching value
    """
    logger.info(f"Loading sequences {list(seqs.keys())}...")
    sql_template = "SELECT pg_catalog.setval('{}', {}, true);"
    sql = "\n".join([sql_template.format(k, v) for k, v in seqs.items()])
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(sql)

    logger.debug("Loaded sequences")


async def compare_data(
    src_pool: Pool, dst_pool: Pool, query: str, tables: list[str], logger: Logger
) -> None:
    """
    Validate data between source and destination databases by doing the following:
    1. Get all tables with primary keys
    2. For each of those tables, select * limit 100
    3. For each row, ensure the row in the destination is identical
    """
    pkeys, _, pkeys_raw = await analyze_table_pkeys(src_pool, logger)

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

    for table in set(pkeys):

        # If specific table list is defined and iterated table is not in that list, skip.
        if tables and (table not in tables):
            continue

        logger.debug(f"Validating table {table}...")
        order_by_pkeys = ",".join(pkeys_dict[table])

        src_rows = await src_pool.fetch(
            query.format(table=table, order_by_pkeys=order_by_pkeys)
        )

        # There is a chance tables are empty...
        if len(src_rows) == 0:
            dst_rows = await dst_pool.fetch(
                query.format(table=table, order_by_pkeys=order_by_pkeys)
            )
            if len(dst_rows) != 0:
                raise AssertionError(
                    f"Table {table} has 0 rows in source but nonzero rows in target... Big problem. Please investigate."
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

        dst_query = f"SELECT * FROM {table} WHERE "

        for k, v in pkey_vals_dict.items():
            dst_query = dst_query + f"{k} IN ({v}) AND "

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
                f'Row count of the sample taken from table "{table}" '
                "does not match in source and destination!\n"
                f"Query: {dst_query}"
            )

        # Check each row for exact match
        for src_row, dst_row in zip(src_rows, dst_rows):
            if src_row != dst_row:
                raise AssertionError(
                    "Row match failure between source and destination.\n"
                    f"Table: {table}\n"
                    f"Source Row: {src_row}\n"
                    f"Dest Row: {dst_row}"
                )

    await src_pool.execute(f"SET extra_float_digits TO {src_old_extra_float_digits};")
    await dst_pool.execute(f"SET extra_float_digits TO {dst_old_extra_float_digits};")
    logger.info(
        "Validation Complete. Samples match in both Source and Destination Databases!"
    )


async def compare_100_rows(
    src_pool: Pool, dst_pool: Pool, tables: list[str], logger: Logger
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

    await compare_data(src_pool, dst_pool, query, tables, logger)


async def compare_latest_100_rows(
    src_pool: Pool, dst_pool: Pool, tables: list[str], logger: Logger
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
    LIMIT 1;
    """

    await compare_data(src_pool, dst_pool, query, tables, logger)


async def table_empty(pool: Pool, table: str, logger: Logger) -> bool:
    """
    return true if the table is empty
    """
    logger.info(f"Checking if table {table} is empty...")
    result = await pool.fetch(f"SELECT * FROM {table} LIMIT 1;")
    return len(result) == 0


async def analyze_table_pkeys(
    pool: Pool, logger: Logger
) -> Tuple[list[str], list[str], Record]:
    """
    return three lists of table names. the first element is all tables
    with pkeys in public and the second is all tables without pkeys in public.
    The third list is the raw rows of the primary key query with the table name,
    constraint name, position and column name for the primary key.
    """
    logger.info("Checking table primary keys...")
    pkeys_raw = await pool.fetch(
        """
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
            AND kcu.table_schema = 'public'
        ORDER BY kcu.table_name,
                position;
        """
    )
    pkeys = [r[0] for r in pkeys_raw]

    all_tables = await pool.fetch(
        """SELECT table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = 'public'
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
    pool: Pool, root_name: str, owner_name: str, logger: Logger
) -> dict:
    """
    Return a dictionary of information about the database used to determine
    whether belt will work on it and what can be migrated.
    """
    logger.info("Checking db requirements...")
    result = {
        "server_version": await pool.fetchval("SHOW server_version;"),
        "max_replication_slots": await pool.fetchval("SHOW max_replication_slots;"),
        "max_worker_processes": await pool.fetchval("SHOW max_worker_processes;"),
        "max_wal_senders": await pool.fetchval("SHOW max_wal_senders;"),
        "shared_preload_libraries": await pool.fetchval(
            "SHOW shared_preload_libraries;"
        ),
        "tables": [],
        "sequences": [],
        "users": [],
    }

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
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;"""
    )

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
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;"""
    )

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
        FROM pg_catalog.pg_roles r
        WHERE r.rolname !~ '^pg_' AND (r.rolname = '{root_name}' OR r.rolname = '{owner_name}')
        ORDER BY 1;"""
    )

    for u in users:
        if u[0] == root_name:
            result["root"] = u
        if u[0] == owner_name:
            result["owner"] = u

    return result
