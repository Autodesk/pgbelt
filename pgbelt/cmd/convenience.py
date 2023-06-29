import socket
from asyncio import open_connection
from asyncio import run
from asyncio import TimeoutError
from asyncio import wait_for
from collections.abc import Awaitable
from logging import Logger

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.config import get_config
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from typer import echo
from typer import Option


def src_dsn(
    db: str,
    dc: str,
    owner: bool = Option(False, help="Use the owner credentials"),
    pglogical: bool = Option(False, help="Use the pglogical credentials."),
) -> None:
    """
    Print a dsn to stdout that you can use to connect to the source db:
    psql "$(dbup src-dsn scribble prod-use1-pg-1)"

    Pass --owner to log in as the owner or --pglogical to log in as pglogical.
    """
    conf = get_config(db, dc, skip_dst=True)
    echo(
        conf.src.owner_dsn
        if owner
        else conf.src.pglogical_dsn
        if pglogical
        else conf.src.root_dsn
    )


def dst_dsn(
    db: str,
    dc: str,
    owner: bool = Option(False, help="Use the owner credentials"),
    pglogical: bool = Option(False, help="Use the pglogical credentials."),
) -> None:
    """
    Print a dsn to stdout that you can use to connect to the destination db:
    psql "$(dbup dst-dsn scribble prod-use1-pg-1)"

    Pass --owner to log in as the owner or --pglogical to log in as pglogical.
    """
    conf = get_config(db, dc, skip_src=True)
    echo(
        conf.dst.owner_dsn
        if owner
        else conf.dst.pglogical_dsn
        if pglogical
        else conf.dst.root_dsn
    )


async def _check_pkeys(
    conf: DbupgradeConfig, logger: Logger
) -> tuple[list[str], list[str]]:
    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        pkey_tables, no_pkey_tables, _ = await analyze_table_pkeys(pool, logger)
    return pkey_tables, no_pkey_tables


def check_pkeys(db: str, dc: str) -> None:
    """
    Print out lists of tables with and without primary keys
    """
    conf = get_config(db, dc, skip_src=True)
    logger = get_logger(db, dc, "convenience.src")
    pkeys, no_pkeys = run(_check_pkeys(conf, logger))
    echo(
        f"""Analyzed table pkeys for {db} in {dc}:
        has pkey: {pkeys}
        no pkey: {no_pkeys}
        """
    )


@run_with_configs()
async def check_connectivity(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Returns exit code 0 if pgbelt can connect to all databases in a datacenter
    (if db is not specified), or to both src and dst of a database.

    This is done by checking network access to the database ports ONLY.

    If any connection times out, the command will exit 1. It will test ALL connections
    before returning exit code 1 or 0, and output which connections passed/failed.
    """

    conf = await config_future

    src_future = open_connection(conf.src.host, conf.src.port)
    src_logger = get_logger(conf.db, conf.dc, "connect.src")
    dst_future = open_connection(conf.dst.host, conf.dst.port)
    dst_logger = get_logger(conf.db, conf.dc, "connect.dst")

    for future, logger in [(src_future, src_logger), (dst_future, dst_logger)]:
        try:
            logger.info("Checking network access to port...")

            # Wait for 3 seconds, then raise TimeoutError
            _, writer = await wait_for(future, timeout=3)
            logger.debug("Can access network port.")
            writer.close()
            await writer.wait_closed()
        except TimeoutError:
            logger.error("Cannot access network port. timed out.")
        except socket.gaierror as e:
            logger.error(f"Socket.gaierror {e}")


COMMANDS = [src_dsn, dst_dsn, check_pkeys, check_connectivity]
