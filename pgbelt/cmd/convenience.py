import socket
from asyncio import open_connection
from asyncio import run
from asyncio import TimeoutError
from asyncio import wait_for
from collections.abc import Awaitable
from logging import Logger

from asyncpg import connect as pg_connect
from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.config import get_config
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from tabulate import tabulate
from typer import echo
from typer import Option
from typer import style


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
        else conf.src.pglogical_dsn if pglogical else conf.src.root_dsn
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
        else conf.dst.pglogical_dsn if pglogical else conf.dst.root_dsn
    )


async def _check_pkeys(
    conf: DbupgradeConfig, logger: Logger
) -> tuple[list[str], list[str]]:
    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        pkey_tables, no_pkey_tables, _ = await analyze_table_pkeys(
            pool, conf.schema_name, logger
        )
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


async def _print_connectivity_results(results: list[dict]):
    """
    For a list of databases in a datacenter, show a table of established connections.

    Also exit(1) if ANY connections failed.
    """

    table = [
        [
            style("database", "yellow"),
            style("src tcp ok", "yellow"),
            style("src query ok", "yellow"),
            style("dst tcp ok", "yellow"),
            style("dst query ok", "yellow"),
        ]
    ]

    results.sort(key=lambda d: d["db"])

    failed_connection_exists = False
    for r in results:
        table.append(
            [
                style(r["db"], "green"),
                style(r["src_tcp"], "green" if r["src_tcp"] else "red"),
                style(r["src_query"], "green" if r["src_query"] else "red"),
                style(r["dst_tcp"], "green" if r["dst_tcp"] else "red"),
                style(r["dst_query"], "green" if r["dst_query"] else "red"),
            ]
        )
        if not failed_connection_exists and (
            not r["src_tcp"]
            or not r["src_query"]
            or not r["dst_tcp"]
            or not r["dst_query"]
        ):
            failed_connection_exists = True

    echo(tabulate(table, headers="firstrow"))

    if failed_connection_exists:
        exit(1)


async def _check_tcp(host: str, port: str, logger: Logger) -> bool:
    try:
        logger.info("Checking network access to port...")
        _, writer = await wait_for(open_connection(host, port), timeout=3)
        logger.debug("Can access network port.")
        writer.close()
        await writer.wait_closed()
        return True
    except TimeoutError:
        logger.error("Cannot access network port. timed out.")
    except socket.gaierror as e:
        logger.error(f"Socket.gaierror {e}")
    except ConnectionRefusedError as e:
        logger.error(f"ConnectionRefusedError {e}")
    return False


async def _check_query(uri: str, logger: Logger) -> bool:
    try:
        logger.info("Checking SELECT 1...")
        conn = await pg_connect(uri, timeout=5)
        try:
            await conn.fetchval("SELECT 1")
            logger.debug("SELECT 1 succeeded.")
            return True
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"SELECT 1 failed: {e}")
    return False


@run_with_configs(results_callback=_print_connectivity_results)
async def check_connectivity(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Returns exit code 0 if pgbelt can connect to all databases in a datacenter
    (if db is not specified), or to both src and dst of a database.

    First checks TCP connectivity to the database ports, then runs a SELECT 1
    using the root credentials to validate authentication and end-to-end
    connectivity.

    If any check fails, the command will exit 1. It will test ALL connections
    before returning exit code 1 or 0, and output which checks passed/failed.
    """

    conf = await config_future

    src_logger = get_logger(conf.db, conf.dc, "connect.src")
    dst_logger = get_logger(conf.db, conf.dc, "connect.dst")

    src_tcp = await _check_tcp(conf.src.ip, conf.src.port, src_logger)
    src_query = await _check_query(conf.src.root_uri, src_logger) if src_tcp else False

    dst_tcp = await _check_tcp(conf.dst.ip, conf.dst.port, dst_logger)
    dst_query = await _check_query(conf.dst.root_uri, dst_logger) if dst_tcp else False

    return {
        "db": conf.db,
        "src_tcp": src_tcp,
        "src_query": src_query,
        "dst_tcp": dst_tcp,
        "dst_query": dst_query,
    }


COMMANDS = [src_dsn, dst_dsn, check_pkeys, check_connectivity]
