from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from logging import Logger

from asyncpg import Pool
from asyncpg.exceptions import DuplicateObjectError
from asyncpg.exceptions import InternalServerError
from asyncpg.exceptions import InvalidParameterValueError
from asyncpg.exceptions import InvalidSchemaNameError
from asyncpg.exceptions import ObjectNotInPrerequisiteStateError
from asyncpg.exceptions import UndefinedFunctionError
from asyncpg.exceptions import UndefinedObjectError
from asyncpg.exceptions import UndefinedTableError
from asyncpg.exceptions import UniqueViolationError

# Key for the advisory lock that serializes changes to the objects every pgbelt
# migration in a database shares: the pglogical role, extension and node.
SHARED_OBJECTS_LOCK_KEY = 7_406_171_123


@asynccontextmanager
async def shared_objects_lock(pool: Pool) -> AsyncIterator[None]:
    """
    Hold a session advisory lock while changing shared pglogical objects, so
    migrations for different schemas in the same database can run in parallel.
    """
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1);", SHARED_OBJECTS_LOCK_KEY)
        try:
            yield
        finally:
            await conn.execute(
                "SELECT pg_advisory_unlock($1);", SHARED_OBJECTS_LOCK_KEY
            )


async def configure_pgl(
    pool: Pool, pgl_pw: str, logger: Logger, owner_user: str
) -> None:
    """
    Set up the pglogical role, grant it superuser and replication, create
    the extension and grant USAGE to its schema to the owner user.
    """
    async with shared_objects_lock(pool):
        await _configure_pgl(pool, pgl_pw, logger, owner_user)


async def _configure_pgl(
    pool: Pool, pgl_pw: str, logger: Logger, owner_user: str
) -> None:
    logger.info("Creating pglogical user and extension...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    f"CREATE ROLE pglogical LOGIN ENCRYPTED PASSWORD '{pgl_pw}';"
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
            await conn.execute(f"GRANT USAGE ON SCHEMA pglogical TO {owner_user};")
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
    pool: Pool,
    tables: list[str],
    schema: str,
    logger: Logger,
    set_name: str = "pgbelt",
) -> None:
    """
    Add each table in the given list to the named replication set
    """
    logger.info(f"Creating new replication set '{set_name}'")
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                f"SELECT pglogical.create_replication_set('{set_name}');"
            )
            logger.debug(f"Created the '{set_name}' replication set")
        except Exception as e:
            logger.debug(f"Could not create replication set '{set_name}': {e}")

    logger.info(
        f"Configuring '{set_name}' replication set with tables from schema {schema}: {tables}"
    )
    for table in tables:
        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(
                        f"SELECT pglogical.replication_set_add_table('{set_name}', '\"{schema}\".\"{table}\"');"
                    )
                    logger.debug(
                        f"Table '{table}' added to '{set_name}' replication set from schema {schema}"
                    )
                except UniqueViolationError:
                    logger.debug(
                        f"Table '{table}' already in '{set_name}' replication set from schema {schema}"
                    )


async def configure_node(pool: Pool, name: str, dsn: str, logger: Logger) -> None:
    """
    Set up a pglogical node. pglogical allows one node per database, so every
    migration in a database shares it.
    """
    logger.info(f"Configuring node {name}...")
    async with shared_objects_lock(pool):
        if await local_node_name(pool) == name:
            logger.debug(f"Node {name} already exists")
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(
                        f"""SELECT pglogical.create_node(
                            node_name:='{name}',
                            dsn:='{dsn}'
                        );"""
                    )
                    logger.debug(f"Node {name} created")
                except InternalServerError as e:
                    if f"node {name} already exists" in str(e):
                        logger.debug(f"Node {name} already exists")
                    else:
                        raise e


async def local_node_name(pool: Pool) -> str | None:
    """
    Name of this database's pglogical node, or None if it has none.
    """
    try:
        return await pool.fetchval(
            """SELECT n.node_name FROM pglogical.local_node l
            JOIN pglogical.node n ON n.node_id = l.node_id;"""
        )
    except (InvalidSchemaNameError, UndefinedTableError):
        return None


async def configure_subscription(
    pool: Pool,
    name: str,
    provider_dsn: str,
    logger: Logger,
    replication_set: str = "pgbelt",
) -> None:
    """
    Set up a subscription
    """
    logger.info(f"Configuring subscription {name}...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    f"""SELECT pglogical.create_subscription(
                        subscription_name:='{name}',
                        replication_sets:='{{{replication_set}}}',
                        provider_dsn:='{provider_dsn}',
                        synchronize_structure:=false,
                        synchronize_data:={'true' if name.startswith('pg1') else 'false'},
                        forward_origins:='{{}}'
                    );"""
                )
                logger.debug(f"Subscription {name} created")
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


async def teardown_replication_set(
    pool: Pool, logger: Logger, set_name: str = "pgbelt"
) -> None:
    """
    Tear down the replication_set
    """
    logger.info(f"Dropping replication set '{set_name}'...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    f"SELECT pglogical.drop_replication_set('{set_name}');"
                )
                logger.debug(f"Replication set '{set_name}' dropped")
            except (
                InvalidSchemaNameError,
                UndefinedFunctionError,
                InternalServerError,
            ):
                logger.debug(f"Replication set '{set_name}' does not exist")
            except ObjectNotInPrerequisiteStateError:
                logger.debug(
                    "pglogical node was already dropped, so we can't drop the replication set. This is okay, keep going."
                )


async def revoke_pgl(
    pool: Pool,
    tables: list[str],
    schema: str,
    logger: Logger,
    drop_role: bool = True,
) -> None:
    """
    Revoke data access permissions from pglogical, and drop the pglogical role
    unless drop_role is False
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

    if not drop_role:
        return

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


async def cleanup_all_pglogical(
    pool: Pool, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Remove all pglogical configuration from a single database, regardless of
    what role (source or destination) it played in a previous migration.

    Tries to drop both subscription names (pg1_pg2 and pg2_pg1) and both node
    names (pg1 and pg2) since we may not know which role the database previously
    played. All teardown functions handle "does not exist" gracefully.
    """
    logger.info("Cleaning up previous pglogical configuration...")

    await teardown_subscription(pool, "pg1_pg2", logger)
    await teardown_subscription(pool, "pg2_pg1", logger)

    await teardown_replication_set(pool, logger)

    await teardown_node(pool, "pg1", logger)
    await teardown_node(pool, "pg2", logger)

    await revoke_pgl(pool, tables, schema, logger)


async def cleanup_leftover_pglogical(
    pool: Pool, node_name: str, tables: list[str], schema: str, logger: Logger
) -> None:
    """
    Cleanup for a migration that may share its database with other migrations.
    Removes everything only if the node was left by a previous migration where this
    database played the other role. Otherwise the shared node and the objects of
    migrations already running are left alone, and setup reuses them.
    """
    async with shared_objects_lock(pool):
        leftover = await local_node_name(pool)
        if leftover is not None and leftover != node_name:
            logger.info(f"Found node {leftover} from a previous migration")
            await cleanup_all_pglogical(pool, tables, schema, logger)


async def teardown_shared_pglogical(
    pool: Pool, node_name: str, tables: list[str], schema: str, logger: Logger
) -> bool:
    """
    Revoke this migration's grants, then drop the node and the pglogical role once
    no pgbelt replication set, subscription or pglogical grant is left in the
    database. Migrations torn down in parallel take turns under the lock, so the
    last one drops them. Returns True if they were dropped.
    """
    async with shared_objects_lock(pool):
        await revoke_pgl(pool, tables, schema, logger, drop_role=False)
        if await pgbelt_objects_remaining(pool) or await pglogical_grants_remaining(
            pool
        ):
            logger.info(
                f"Other pgbelt migrations still use node {node_name}, keeping it"
            )
            return False
        await teardown_node(pool, node_name, logger)
        await revoke_pgl(pool, tables, schema, logger)
        return True


async def subscription_status(pool: Pool, logger: Logger, name: str) -> str:
    """
    Get the status of the named subscription.
    Status can be initializing, replicating, down, or unconfigured.
    """
    logger.debug(f"checking subscription {name} status")
    try:
        status = await pool.fetchval(
            """SELECT status FROM pglogical.show_subscription_status()
            WHERE subscription_name = $1;""",
            name,
        )
        return status or "unconfigured"
    except (
        InvalidSchemaNameError,
        UndefinedFunctionError,
        ObjectNotInPrerequisiteStateError,
    ):
        return "unconfigured"


async def pgbelt_objects_remaining(pool: Pool) -> int:
    """
    Count the pgbelt replication sets and subscriptions left in a database. The
    shared node, role and extension should only be dropped once this is zero.
    """
    try:
        return await pool.fetchval("""SELECT
                (SELECT count(*) FROM pglogical.replication_set
                    WHERE set_name LIKE 'pgbelt%')
                + (SELECT count(*) FROM pglogical.subscription
                    WHERE sub_name LIKE 'pg1\\_pg2%' OR sub_name LIKE 'pg2\\_pg1%');""")
    except (InvalidSchemaNameError, UndefinedTableError):
        return 0


async def pglogical_grants_remaining(pool: Pool) -> int:
    """
    Count the tables and sequences in this database the pglogical role still has
    privileges on, for example from other schemas still being migrated.
    """
    return await pool.fetchval(
        """SELECT count(*) FROM pg_class c, aclexplode(c.relacl) a
        WHERE a.grantee = (SELECT oid FROM pg_roles WHERE rolname = 'pglogical');"""
    )


async def src_status(
    pool: Pool,
    logger: Logger,
    forward_subscription: str = "pg1_pg2",
    back_subscription: str = "pg2_pg1",
) -> dict[str, str]:
    """
    Get the status of the back replication subscription and the forward replication lag
    """
    logger.info("checking source status...")
    status = {"pg2_pg1": await subscription_status(pool, logger, back_subscription)}

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
                FROM pg_stat_replication WHERE application_name = $1;""",
            forward_subscription,
        )
    else:
        lag_data = await pool.fetchrow(
            """
            SELECT current_timestamp, application_name,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.sent_lsn) AS sent_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.write_lsn) AS write_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.flush_lsn) AS flush_location_lag,
                pg_wal_lsn_diff(pg_current_wal_lsn(), pg_stat_replication.replay_lsn) AS replay_location_lag
                FROM pg_stat_replication WHERE application_name = $1;""",
            forward_subscription,
        )

    status["sent_lag"] = str(lag_data[2]) if lag_data else "unknown"
    status["write_lag"] = str(lag_data[3]) if lag_data else "unknown"
    status["flush_lag"] = str(lag_data[4]) if lag_data else "unknown"
    status["replay_lag"] = str(lag_data[5]) if lag_data else "unknown"

    return status


async def dst_status(
    pool: Pool, logger: Logger, forward_subscription: str = "pg1_pg2"
) -> dict[str, str]:
    """
    Get the status of the forward replication subscription
    """
    logger.info("checking target status...")
    return {"pg1_pg2": await subscription_status(pool, logger, forward_subscription)}
