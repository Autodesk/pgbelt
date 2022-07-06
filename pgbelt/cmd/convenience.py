from asyncio import run
from logging import Logger
from typing import Tuple

from asyncpg import create_pool
from typer import echo
from typer import Option

from pgbelt.config.config import get_config
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys


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
) -> Tuple[list[str], list[str]]:
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


COMMANDS = [src_dsn, dst_dsn, check_pkeys]
