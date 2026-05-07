import socket
from asyncio import gather
from asyncio import open_connection
from asyncio import run
from asyncio import TimeoutError
from asyncio import wait_for
from collections.abc import Awaitable
from logging import Logger

import asyncpg
from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.config import get_config
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dblink import check_connectivity_via_dblink
from pgbelt.util.dblink import ensure_dblink
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from tabulate import tabulate
from typer import echo
from typer import Option
from typer import style

# Short connect+query timeout used for the owner-credential probe.
# A healthy asyncpg connect against RDS completes in <500ms even
# cross-account; 10s is comfortably above that and 6x faster than
# asyncpg's 60s default ``connect_timeout``, so a server that silently
# closes a bad-password connection (which is what RDS does) still
# surfaces a structured failure on the connectivity grid instead of
# wedging the SFN at the next belt stage.
_OWNER_PROBE_TIMEOUT_SECONDS = 10.0


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
            style("src owner ok", "yellow"),
            style("src->dst dblink ok", "yellow"),
            style("dst tcp ok", "yellow"),
            style("dst query ok", "yellow"),
            style("dst owner ok", "yellow"),
            style("dst->src dblink ok", "yellow"),
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
                style(
                    r["src_owner_query"],
                    "green" if r["src_owner_query"] else "red",
                ),
                style(
                    r["src_to_dst_dblink"],
                    "green" if r["src_to_dst_dblink"] else "red",
                ),
                style(r["dst_tcp"], "green" if r["dst_tcp"] else "red"),
                style(r["dst_query"], "green" if r["dst_query"] else "red"),
                style(
                    r["dst_owner_query"],
                    "green" if r["dst_owner_query"] else "red",
                ),
                style(
                    r["dst_to_src_dblink"],
                    "green" if r["dst_to_src_dblink"] else "red",
                ),
            ]
        )
        if not failed_connection_exists and not all(
            [
                r["src_tcp"],
                r["src_query"],
                r["src_owner_query"],
                r["src_to_dst_dblink"],
                r["dst_tcp"],
                r["dst_query"],
                r["dst_owner_query"],
                r["dst_to_src_dblink"],
            ]
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


async def _check_owner_query(
    owner_uri: str,
    label: str,
    logger: Logger,
    timeout: float = _OWNER_PROBE_TIMEOUT_SECONDS,
) -> bool:
    """Validate owner credentials by opening one asyncpg connection
    against ``owner_uri`` and running ``SELECT 1`` -- with a short
    timeout so a silent server-side reset on a bad/stale owner password
    fails fast instead of consuming asyncpg's 60s default.

    Returns True on success. Logs the underlying error and returns False
    on any failure -- the caller surfaces this in the JSON output as
    ``{src,dst}_owner_query: false`` (and on the per-DB ``failed_checks``
    list), giving operators a concrete pointer to a credential issue.
    """
    logger.info("Checking %s SELECT 1 (timeout=%gs)...", label, timeout)
    conn = None
    try:
        conn = await asyncpg.connect(owner_uri, timeout=timeout)
        await wait_for(conn.fetchval("SELECT 1"), timeout=timeout)
        logger.debug("%s SELECT 1 succeeded.", label)
        return True
    except TimeoutError:
        # Distinct from the generic Exception branch because a timeout
        # is the smoking gun for a wrong owner password against RDS:
        # the server stays silent and asyncpg keeps retrying until the
        # configured connect_timeout elapses.
        logger.error(
            "%s SELECT 1 timed out after %gs (owner password likely wrong/stale)",
            label,
            timeout,
        )
        return False
    except Exception as e:
        logger.error("%s SELECT 1 failed: %s: %s", label, type(e).__name__, e)
        return False
    finally:
        if conn is not None:
            try:
                await conn.close(timeout=2)
            except Exception as close_exc:
                # The probe's whole job is to answer "can owner
                # authenticate?"; a stuck close is unrelated noise we
                # don't want to mask the verdict with. Log at debug so
                # we still leave a breadcrumb for the rare case where
                # the server is wedged badly enough to also block close.
                logger.debug(
                    "%s connection close failed (ignored): %s",
                    label,
                    close_exc,
                )


@run_with_configs(results_callback=_print_connectivity_results)
async def check_connectivity(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Returns exit code 0 if pgbelt can connect to all databases in a datacenter
    (if db is not specified), or to both src and dst of a database.

    Runs four checks per side:
    1. TCP connectivity to the database port (from pgbelt).
    2. SELECT 1 via a root-credentials connection pool (validates root creds
    from pgbelt).
    3. SELECT 1 via the owner-credentials URI with a 10s timeout (validates
    owner creds; without this, a wrong/stale owner password would be caught
    much later inside ``belt precheck`` as a 60s asyncpg pool-creation hang).
    4. dblink from src->dst and dst->src (validates that the database servers
    themselves can reach each other -- the same network path pglogical uses).

    The dblink extension is created if not present and left in place. It is
    removed by belt teardown --full.

    If any check fails, the command will exit 1. It will test ALL connections
    before returning exit code 1 or 0, and output which checks passed/failed.
    """

    conf = await config_future

    src_logger = get_logger(conf.db, conf.dc, "connect.src")
    dst_logger = get_logger(conf.db, conf.dc, "connect.dst")

    src_tcp = await _check_tcp(conf.src.ip, conf.src.port, src_logger)
    dst_tcp = await _check_tcp(conf.dst.ip, conf.dst.port, dst_logger)

    src_query = False
    dst_query = False
    src_owner_query = False
    dst_owner_query = False
    src_to_dst_dblink = False
    dst_to_src_dblink = False

    if src_tcp and dst_tcp:
        pools = await gather(
            create_pool(conf.src.root_uri, min_size=1),
            create_pool(conf.dst.root_uri, min_size=1),
        )
        src_pool, dst_pool = pools

        try:
            # SELECT 1 to validate credentials from pgbelt's perspective
            try:
                await src_pool.fetchval("SELECT 1")
                src_query = True
                src_logger.debug("SELECT 1 succeeded.")
            except Exception as e:
                src_logger.error(f"SELECT 1 failed: {e}")

            try:
                await dst_pool.fetchval("SELECT 1")
                dst_query = True
                dst_logger.debug("SELECT 1 succeeded.")
            except Exception as e:
                dst_logger.error(f"SELECT 1 failed: {e}")

            # Owner-credential probes -- gated on the matching root
            # SELECT 1 because owner sits behind the same network
            # path; if root can't query, owner won't either, and we'd
            # rather surface the underlying network/RDS problem on the
            # ``*_query`` cell than double-flag it.
            if src_query:
                src_owner_query = await _check_owner_query(
                    conf.src.owner_uri, "src owner", src_logger
                )
            if dst_query:
                dst_owner_query = await _check_owner_query(
                    conf.dst.owner_uri, "dst owner", dst_logger
                )

            # dblink cross-host checks
            if src_query and dst_query:
                await gather(
                    ensure_dblink(src_pool, src_logger),
                    ensure_dblink(dst_pool, dst_logger),
                )

                src_to_dst_dblink = await check_connectivity_via_dblink(
                    src_pool, conf.dst.root_dsn, src_logger
                )
                dst_to_src_dblink = await check_connectivity_via_dblink(
                    dst_pool, conf.src.root_dsn, dst_logger
                )
        finally:
            await gather(*[p.close() for p in pools])
    elif src_tcp:
        async with create_pool(conf.src.root_uri, min_size=1) as src_pool:
            try:
                await src_pool.fetchval("SELECT 1")
                src_query = True
                src_logger.debug("SELECT 1 succeeded.")
            except Exception as e:
                src_logger.error(f"SELECT 1 failed: {e}")
        if src_query:
            src_owner_query = await _check_owner_query(
                conf.src.owner_uri, "src owner", src_logger
            )
    elif dst_tcp:
        async with create_pool(conf.dst.root_uri, min_size=1) as dst_pool:
            try:
                await dst_pool.fetchval("SELECT 1")
                dst_query = True
                dst_logger.debug("SELECT 1 succeeded.")
            except Exception as e:
                dst_logger.error(f"SELECT 1 failed: {e}")
        if dst_query:
            dst_owner_query = await _check_owner_query(
                conf.dst.owner_uri, "dst owner", dst_logger
            )

    return {
        "db": conf.db,
        "src_tcp": src_tcp,
        "src_query": src_query,
        "src_owner_query": src_owner_query,
        "src_to_dst_dblink": src_to_dst_dblink,
        "dst_tcp": dst_tcp,
        "dst_query": dst_query,
        "dst_owner_query": dst_owner_query,
        "dst_to_src_dblink": dst_to_src_dblink,
    }


COMMANDS = [src_dsn, dst_dsn, check_pkeys, check_connectivity]
