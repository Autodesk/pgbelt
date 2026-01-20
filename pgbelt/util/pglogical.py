from logging import Logger

from asyncpg import Pool
from asyncpg.exceptions import DuplicateObjectError
from asyncpg.exceptions import InternalServerError
from asyncpg.exceptions import InvalidParameterValueError
from asyncpg.exceptions import InvalidSchemaNameError
from asyncpg.exceptions import ObjectNotInPrerequisiteStateError
from asyncpg.exceptions import UndefinedFunctionError
from asyncpg.exceptions import UndefinedObjectError
from asyncpg.exceptions import UniqueViolationError
from asyncpg.exceptions import UndefinedTableError


async def configure_pgl(
    pool: Pool, pgl_pw: str, logger: Logger, owner_user: str
) -> None:
    """
    Set up the pglogical role, grant it superuser and replication, create
    the extension and grant USAGE to its schema to the owner user.
    """
    logger.info("Creating pglogical user and extension...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                pw_literal = await conn.fetchval(
                    "SELECT quote_literal($1::text);", pgl_pw
                )
                await conn.execute(
                    f"CREATE ROLE pglogical LOGIN ENCRYPTED PASSWORD {pw_literal};"
                )
                logger.debug("pglogical user created")
            except DuplicateObjectError:
                logger.debug("pglogical user already created")

    # Check if the database is RDS
    async with pool.acquire() as conn:
        async with conn.transaction():
            pg_roles = await conn.fetch("SELECT rolname FROM pg_roles;")

    is_rds = "rdsadmin" in [i[0] for i in pg_roles]

    # If this is an RDS Database, grant rds_superuser and rds_replication
    if is_rds:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("GRANT rds_superuser TO pglogical;")
                await conn.execute("GRANT rds_replication TO pglogical;")
    # If this is not an RDS database, just ensure the user is a superuser
    else:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("ALTER USER pglogical WITH SUPERUSER;")

    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("CREATE EXTENSION pglogical;")
                logger.debug("pglogical extension created")
            except DuplicateObjectError:
                logger.debug("pglogical extension already created")

    # TODO: Somehow test for this working in our integration test.
    #     We need to make the DBs have a separate schema owner role to test this.
    async with pool.acquire() as conn:
        async with conn.transaction():
            owner_ident = await conn.fetchval(
                "SELECT quote_ident($1::text);", owner_user
            )
            await conn.execute(f"GRANT USAGE ON SCHEMA pglogical TO {owner_ident};")
            logger.debug(
                f"GRANTed USAGE ON pglogical schema to Schema Owner {owner_user}"
            )


async def grant_pgl(pool: Pool, tables: list[str], schema: str, logger: Logger) -> None:
    """
    Grant pglogical access to the data

    TODO: This should instead find all tables and sequences owned by the currently connected user
    and grant to each individually. Then we can call this for every known dsn and we won't miss
    any grants to pglogical because of weird ownership stuff.
    """
    logger.info("Granting data permissions to pglogical...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            if tables:
                tables_with_schema = [f'{schema}."{table}"' for table in tables]
                await conn.execute(
                    f"GRANT ALL ON TABLE {','.join(tables_with_schema)} TO pglogical;"
                )
            else:
                await conn.execute(
                    f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO pglogical;"
                )
            await conn.execute(
                f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO pglogical;"
            )
            logger.debug("pglogical data grants complete")


async def configure_replication_set(
    pool: Pool, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Add each table in the given list to the default replication set
    """
    logger.info("Creating new replication set 'pgbelt'")
    async with pool.acquire() as conn:
        try:
            await conn.execute("SELECT pglogical.create_replication_set('pgbelt');")
            logger.debug("Created the 'pgbelt' replication set")
        except Exception as e:
            logger.debug(f"Could not create replication set 'pgbelt': {e}")

    logger.info(
        f"Configuring 'pgbelt' replication set with tables from schema {schema}: {tables}"
    )
    for table in tables:
        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(
                        "SELECT pglogical.replication_set_add_table('pgbelt', format('%I.%I', $1::text, $2::text)::regclass);",
                        schema,
                        table,
                    )
                    logger.debug(
                        f"Table '{table}' added to 'pgbelt' replication set from schema {schema}"
                    )
                except UniqueViolationError:
                    logger.debug(
                        f"Table '{table}' already in 'pgbelt' replication set from schema {schema}"
                    )


async def configure_node(pool: Pool, name: str, dsn: str, logger: Logger) -> None:
    """
    Set up a pglogical node
    """
    logger.info(f"Configuring node {name}...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    "SELECT pglogical.create_node(node_name := $1, dsn := $2);",
                    name,
                    dsn,
                )
                logger.debug(f"Node {name} created")
            except InternalServerError as e:
                if f"node {name} already exists" in str(e):
                    logger.debug(f"Node {name} already exists")
                else:
                    raise e


async def configure_subscription(
    pool: Pool, name: str, provider_dsn: str, logger: Logger
) -> None:
    """
    Set up a subscription
    """
    logger.info(f"Configuring subscription {name}...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    """
                    SELECT pglogical.create_subscription(
                        subscription_name := $1,
                        replication_sets := ARRAY['pgbelt']::text[],
                        provider_dsn := $2,
                        synchronize_structure := $3,
                        synchronize_data := $4,
                        forward_origins := ARRAY[]::text[],
                        force_text_transfer := $5
                    );
                    """,
                    name,
                    provider_dsn,
                    False,
                    name.startswith("pg1"),
                    name.startswith("pg2"),
                )
                logger.debug(f"Subscription {name} created")
            except UndefinedFunctionError:
                await conn.execute(
                    """
                    SELECT pglogical.create_subscription(
                        subscription_name := $1,
                        replication_sets := ARRAY['pgbelt']::text[],
                        provider_dsn := $2,
                        synchronize_structure := $3,
                        synchronize_data := $4,
                        forward_origins := ARRAY[]::text[]
                    );
                    """,
                    name,
                    provider_dsn,
                    False,
                    name.startswith("pg1"),
                )
                logger.debug(f"Subscription {name} created (no force_text_transfer)")
            except InvalidParameterValueError as e:
                if f'existing subscription "{name}"' in str(e):
                    logger.debug(f"Subscription {name} already exists")
                else:
                    raise e


async def teardown_subscription(pool: Pool, name: str, logger: Logger) -> None:
    """
    Tear down a subscription
    """
    logger.info(f"Dropping subscription {name}...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    f"SELECT pglogical.drop_subscription('{name}', true);"
                )
                logger.debug(f"Subscription {name} dropped")
            except (InvalidSchemaNameError, UndefinedFunctionError):
                logger.debug(f"Subscription {name} does not exist")


async def teardown_node(pool: Pool, name: str, logger: Logger) -> None:
    """
    Tear down a node
    """
    logger.info(f"Dropping node {name}...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(f"SELECT pglogical.drop_node('{name}', true);")
                logger.debug(f"Node {name} dropped")
            except (InvalidSchemaNameError, UndefinedFunctionError):
                logger.debug(f"Node {name} does not exist")


async def teardown_replication_set(pool: Pool, logger: Logger) -> None:
    """
    Tear down the replication_set
    """
    logger.info("Dropping replication set 'pgbelt'...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("SELECT pglogical.drop_replication_set('pgbelt');")
                logger.debug("Replication set 'pgbelt' dropped")
            except (
                InvalidSchemaNameError,
                UndefinedFunctionError,
                InternalServerError,
            ):
                logger.debug("Replication set 'pgbelt' does not exist")
            except ObjectNotInPrerequisiteStateError:
                logger.debug(
                    "pglogical node was already dropped, so we can't drop the replication set. This is okay, keep going."
                )


async def revoke_pgl(
    pool: Pool, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Revoke data access permissions from pglogical, and drop the pglogical role
    """
    logger.info("Revoking data access permissions from pglogical...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM pglogical;"
                )
                await conn.execute(
                    f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM pglogical;"
                )
                logger.debug("Data access permissions revoked")
            except UndefinedObjectError as e:
                if 'role "pglogical" does not exist' in str(e):
                    logger.debug("pglogical does not exist")
                else:
                    raise e

    logger.info("Dropping pglogical role...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP ROLE IF EXISTS pglogical;")
            logger.debug("Pglogical role dropped")


async def teardown_pgl(pool: Pool, logger: Logger) -> None:
    """
    If they exist, drop the pglogical extension
    """
    logger.info("Dropping pglogical extension...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP EXTENSION IF EXISTS pglogical;")
            logger.debug("Pglogical extension dropped")


async def subscription_status(pool: Pool, logger: Logger) -> str:
    """
    Get the status of a subscription. Assumes one subscription in a db.
    Status can be initializing, replicating, down, or unconfigured.
    """
    logger.debug("checking subscription status")
    try:
        subscription_data = await pool.fetchval(
            "SELECT pglogical.show_subscription_status();"
        )
        if not subscription_data:
            return "unconfigured"
        return subscription_data[1]
    except (
        InvalidSchemaNameError,
        UndefinedFunctionError,
        ObjectNotInPrerequisiteStateError,
    ):
        return "unconfigured"


def _sanitize_pglogical_record(record: object) -> dict:
    data = dict(record)
    for key in list(data.keys()):
        if "dsn" in key.lower() or "password" in key.lower():
            data.pop(key, None)
    return data


def _sanitize_activity_record(record: object) -> dict:
    data = dict(record)
    query = data.get("query")
    if isinstance(query, str) and "password" in query.lower():
        data["query"] = "[redacted]"
    return data


async def subscription_diagnostics(
    pool: Pool, logger: Logger, subscription_name: str | None = None
) -> dict[str, list[dict]]:
    """
    Collect best-effort diagnostics from pglogical to help identify failures.
    Strips DSNs and secrets from results.
    """
    diagnostics: dict[str, list[dict]] = {}

    try:
        status_rows = await pool.fetch(
            "SELECT * FROM pglogical.show_subscription_status();"
        )
        status_data = [_sanitize_pglogical_record(r) for r in status_rows]
        if subscription_name:
            status_data = [
                r
                for r in status_data
                if r.get("subscription_name") == subscription_name
                or r.get("sub_name") == subscription_name
                or r.get("name") == subscription_name
            ]
        if status_data:
            diagnostics["subscription_status"] = status_data
    except (
        InvalidSchemaNameError,
        UndefinedFunctionError,
        ObjectNotInPrerequisiteStateError,
    ):
        logger.debug("pglogical show_subscription_status is unavailable")

    for label, query in (
        ("subscription", "SELECT * FROM pglogical.subscription;"),
        ("subscription_status", "SELECT * FROM pglogical.subscription_status;"),
        ("apply_status", "SELECT * FROM pglogical.apply_status;"),
    ):
        try:
            rows = await pool.fetch(query)
            data = [_sanitize_pglogical_record(r) for r in rows]
            if subscription_name:
                data = [
                    r
                    for r in data
                    if r.get("subscription_name") == subscription_name
                    or r.get("sub_name") == subscription_name
                ]
            if data:
                diagnostics[label] = data
        except (
            InvalidSchemaNameError,
            UndefinedFunctionError,
            ObjectNotInPrerequisiteStateError,
            UndefinedObjectError,
            UndefinedTableError,
        ):
            logger.debug(f"pglogical {label} is unavailable")

    try:
        sync_rows = await pool.fetch("SELECT * FROM pglogical.local_sync_status;")
        sync_data = [_sanitize_pglogical_record(r) for r in sync_rows]
        if subscription_name:
            sync_data = [
                r
                for r in sync_data
                if r.get("sub_name") == subscription_name
                or r.get("subscription_name") == subscription_name
            ]
        if sync_data:
            diagnostics["local_sync_status"] = sync_data
    except (
        InvalidSchemaNameError,
        UndefinedFunctionError,
        ObjectNotInPrerequisiteStateError,
    ):
        logger.debug("pglogical local_sync_status is unavailable")

    return diagnostics


async def replication_activity(
    pool: Pool, logger: Logger, app_prefix: str
) -> list[dict]:
    try:
        rows = await pool.fetch(
            """
            SELECT pid, application_name, state, query
            FROM pg_stat_activity
            WHERE application_name LIKE $1 OR query LIKE 'START_REPLICATION%';
            """,
            f"{app_prefix}%",
        )
        return [_sanitize_activity_record(r) for r in rows]
    except Exception as e:
        logger.debug(f"pg_stat_activity lookup failed: {e}")
        return []


async def src_status(pool: Pool, logger: Logger) -> dict[str, str]:
    """
    Get the status of the back replication subscription and the forward replication lag
    """
    logger.info("checking source status...")
    status_value = await subscription_status(pool, logger)
    status = {"pg2_pg1": status_value}
    if status_value == "down":
        diagnostics = await subscription_diagnostics(pool, logger, "pg2_pg1")
        if diagnostics:
            logger.error(f"pglogical diagnostics (pg2_pg1): {diagnostics}")

    server_version = await pool.fetchval("SHOW server_version;")

    logger.debug("checking source to target lag")
    if "9.6" in server_version:
        lag_data = await pool.fetchrow(
            """
            SELECT current_timestamp, application_name,
                pg_xlog_location_diff(pg_current_xlog_location(), pg_stat_replication.sent_location) AS sent_location_lag,
                pg_xlog_location_diff(pg_current_xlog_location(), pg_stat_replication.write_location) AS write_location_lag,
                pg_xlog_location_diff(pg_current_xlog_location(), pg_stat_replication.flush_location) AS flush_location_lag,
                pg_xlog_location_diff(pg_current_xlog_location(), pg_stat_replication.replay_location) AS replay_location_lag
                FROM pg_stat_replication WHERE application_name = 'pg1_pg2';"""
        )
    else:
        lag_data = await pool.fetchrow(
            """
            SELECT current_timestamp, application_name,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.sent_lsn) AS sent_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.write_lsn) AS write_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.flush_lsn) AS flush_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.replay_lsn) AS replay_location_lag
                FROM pg_stat_replication WHERE application_name = 'pg1_pg2';"""
        )

    status["sent_lag"] = str(lag_data[2]) if lag_data else "unknown"
    status["write_lag"] = str(lag_data[3]) if lag_data else "unknown"
    status["flush_lag"] = str(lag_data[4]) if lag_data else "unknown"
    status["replay_lag"] = str(lag_data[5]) if lag_data else "unknown"

    return status


async def dst_status(pool: Pool, logger: Logger) -> dict[str, str]:
    """
    Get the status of the forward replication subscription
    """
    logger.info("checking target status...")
    status_value = await subscription_status(pool, logger)
    if status_value == "down":
        diagnostics = await subscription_diagnostics(pool, logger, "pg1_pg2")
        if diagnostics:
            logger.error(f"pglogical diagnostics (pg1_pg2): {diagnostics}")
    return {"pg1_pg2": status_value}
