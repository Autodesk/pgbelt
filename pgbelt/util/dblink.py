from logging import Logger

from asyncpg import Pool
from asyncpg.exceptions import DuplicateObjectError


async def ensure_dblink(pool: Pool, logger: Logger) -> None:
    """
    Create the dblink extension if it does not already exist.
    """
    logger.info("Ensuring dblink extension exists...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS dblink;")
                logger.debug("dblink extension ready")
            except DuplicateObjectError:
                logger.debug("dblink extension already exists")


async def teardown_dblink(pool: Pool, logger: Logger) -> None:
    """
    Drop the dblink extension if it exists.
    """
    logger.info("Dropping dblink extension...")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DROP EXTENSION IF EXISTS dblink;")
            logger.debug("dblink extension dropped")


async def check_connectivity_via_dblink(
    pool: Pool, remote_dsn: str, logger: Logger
) -> bool:
    """
    From the database behind `pool`, attempt a dblink connection to `remote_dsn`
    and run SELECT 1. This validates that the database server itself can reach
    the remote host over the network and authenticate.
    """
    logger.info("Checking cross-host connectivity via dblink...")
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchval(
                "SELECT dblink_connect('connectivity_check', $1);", remote_dsn
            )
            if result == "OK":
                logger.debug("dblink connection succeeded")
                await conn.execute("SELECT dblink_disconnect('connectivity_check');")
                return True
            logger.error(f"dblink_connect returned unexpected result: {result}")
            return False
        except Exception as e:
            logger.error(f"dblink connectivity check failed: {e}")
            return False
