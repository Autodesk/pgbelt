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


async def configure_pgl(pool: Pool, pgl_pw: str, logger: Logger) -> None:
    """
    Set up the pglogical role, grant it superuser and replication, and create
    the extension.
    """
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


async def grant_pgl(pool: Pool, tables: list[str], logger: Logger) -> None:
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
                await conn.execute(
                    f"GRANT ALL ON TABLE {','.join(tables)} TO pglogical;"
                )
            else:
                await conn.execute(
                    "GRANT ALL ON ALL TABLES IN SCHEMA public TO pglogical;"
                )
            await conn.execute(
                "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO pglogical;"
            )
            logger.debug("pglogical data grants complete")


async def configure_replication_set(
    pool: Pool, tables: list[str], logger: Logger
) -> None:
    """
    Add each table in the given list to the default replication set
    """
    logger.info(f"Configuring default replication set with tables: {tables}")
    for table in tables:
        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(
                        f"SELECT pglogical.replication_set_add_table('default', '{table}');"
                    )
                    logger.debug(f"{table} added to default replication set")
                except UniqueViolationError:
                    logger.debug(f"{table} already in default replication set")


async def configure_node(pool: Pool, name: str, dsn: str, logger: Logger) -> None:
    """
    Set up a pglogical node
    """
    logger.info(f"Configuring node {name}...")
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
                    f"""SELECT pglogical.create_subscription(
                        subscription_name:='{name}',
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


async def teardown_replication_set(pool: Pool, logger: Logger) -> None:
    """
    Tear down the replication_set
    """
    logger.info("Dropping replication set 'default'...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("SELECT pglogical.drop_replication_set('default');")
                logger.debug("Replication set 'default' dropped")
            except (
                InvalidSchemaNameError,
                UndefinedFunctionError,
                InternalServerError,
            ):
                logger.debug("Replication set 'default' does not exist")


async def revoke_pgl(pool: Pool, tables: list[str], logger: Logger) -> None:
    """
    Revoke data access permissions from pglogical
    """
    logger.info("Revoking data access permissions from pglogical...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                if tables:
                    await conn.execute(
                        f"REVOKE ALL ON TABLE {','.join(tables)} FROM pglogical;"
                    )
                else:
                    await conn.execute(
                        "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM pglogical;"
                    )
                await conn.execute(
                    "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM pglogical;"
                )
                logger.debug("Data access permissions revoked")
            except UndefinedObjectError as e:
                if 'role "pglogical" does not exist' in str(e):
                    logger.debug("pglogical does not exist")
                else:
                    raise e


async def teardown_pgl(pool: Pool, logger: Logger) -> None:
    """
    If they exist, drop the pglogical extension and then the user
    """
    logger.info("Dropping pglogical extension...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP EXTENSION IF EXISTS pglogical;")
            logger.debug("Pglogical extension dropped")

    logger.info("Dropping pglogical user...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP ROLE IF EXISTS pglogical;")
            logger.debug("Pglogical user dropped")


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


async def src_status(pool: Pool, logger: Logger) -> dict[str, str]:
    """
    Get the status of the back replication subscription and the forward replication lag
    """
    logger.info("checking source status...")
    status = {"pg2_pg1": await subscription_status(pool, logger)}

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
    return {"pg1_pg2": await subscription_status(pool, logger)}
