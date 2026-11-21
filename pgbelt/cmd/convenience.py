from asyncio import gather
from asyncio import open_connection
from asyncio import run
from asyncio import TimeoutError
from asyncio import wait_for
from collections.abc import Awaitable
from dataclasses import dataclass
from dataclasses import field
from logging import Logger

import asyncpg
from asyncpg import create_pool
from asyncpg import Pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.config import get_config
from pgbelt.config.models import DbConfig
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


_CHECK_COLUMNS = [
    ("src_tcp", "src tcp ok"),
    ("src_query", "src query ok"),
    ("src_owner_query", "src owner ok"),
    ("src_to_dst_dblink", "src->dst dblink ok"),
    ("dst_tcp", "dst tcp ok"),
    ("dst_query", "dst query ok"),
    ("dst_owner_query", "dst owner ok"),
    ("dst_to_src_dblink", "dst->src dblink ok"),
]


async def _print_connectivity_results(results: list[dict]):
    """
    For a list of databases in a datacenter, show a table of established connections.

    Each failed check is listed under the table with its error text, so you can
    tell which side rejected the connection.

    Also exit(1) if ANY connections failed.
    """

    table = [
        [style("database", "yellow")]
        + [style(label, "yellow") for _, label in _CHECK_COLUMNS]
    ]

    results.sort(key=lambda d: d["db"])

    failed_connection_exists = False
    error_lines = []
    for r in results:
        table.append(
            [style(r["db"], "green")]
            + [
                style(r[check], "green" if r[check] else "red")
                for check, _ in _CHECK_COLUMNS
            ]
        )
        failed = [check for check, _ in _CHECK_COLUMNS if not r[check]]
        if failed:
            failed_connection_exists = True
        errors = r.get("errors") or {}
        for check in failed:
            side = check.split("_", 1)[0]
            host = r.get(f"{side}_host")
            where = f" ({side} host {host})" if host else ""
            detail = errors.get(check, "failed")
            error_lines.append(f"{r['db']}: {check}{where}: {detail}")

    output = tabulate(table, headers="firstrow")
    if error_lines:
        output += (
            "\n\n" + style("Failed checks:", "red") + "\n" + "\n".join(error_lines)
        )
    echo(output)

    if failed_connection_exists:
        exit(1)


def _format_error(e: BaseException) -> str:
    """Render an exception as ``Type: message`` plus the Postgres DETAIL, if
    any. dblink puts the remote server's actual error in DETAIL."""
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    detail = getattr(e, "detail", None)
    if isinstance(detail, str) and detail:
        text += f" ({detail.strip()})"
    return text


async def _check_tcp(host: str, port: str, logger: Logger) -> tuple[bool, str | None]:
    """Open and close a TCP connection to ``host:port``.

    Returns ``(ok, error)``. ``error`` is None on success.
    """
    try:
        logger.info("Checking network access to port...")
        _, writer = await wait_for(open_connection(host, port), timeout=3)
        logger.debug("Can access network port.")
        writer.close()
        await writer.wait_closed()
        return True, None
    except TimeoutError:
        # TimeoutError is an OSError subclass, so this has to come first.
        error = f"timed out connecting to {host}:{port} after 3s"
    except OSError as e:
        # Covers socket.gaierror, ConnectionRefusedError and unreachable hosts.
        error = f"{_format_error(e)} connecting to {host}:{port}"
    logger.error(f"Cannot access network port: {error}")
    return False, error


async def _check_owner_query(
    owner_uri: str,
    label: str,
    logger: Logger,
    timeout: float = _OWNER_PROBE_TIMEOUT_SECONDS,
) -> tuple[bool, str | None]:
    """Validate owner credentials by opening one asyncpg connection
    against ``owner_uri`` and running ``SELECT 1`` -- with a short
    timeout so a silent server-side reset on a bad/stale owner password
    fails fast instead of consuming asyncpg's 60s default.

    Returns ``(True, None)`` on success. On any failure it logs the
    underlying error and returns ``(False, error_text)`` -- the caller
    surfaces this in the JSON output as ``{src,dst}_owner_query: false``
    (and on the per-DB ``failed_checks`` list and ``errors`` map), giving
    operators a concrete pointer to a credential issue.
    """
    logger.info("Checking %s SELECT 1 (timeout=%gs)...", label, timeout)
    conn = None
    try:
        conn = await asyncpg.connect(owner_uri, timeout=timeout)
        await wait_for(conn.fetchval("SELECT 1"), timeout=timeout)
        logger.debug("%s SELECT 1 succeeded.", label)
        return True, None
    except TimeoutError:
        # Distinct from the generic Exception branch because a timeout
        # is the smoking gun for a wrong owner password against RDS:
        # the server stays silent and asyncpg keeps retrying until the
        # configured connect_timeout elapses.
        error = (
            f"owner SELECT 1 timed out after {timeout:g}s "
            "(owner password likely wrong/stale)"
        )
        logger.error("%s %s", label, error)
        return False, error
    except Exception as e:
        error = f"owner SELECT 1 failed: {_format_error(e)}"
        logger.error("%s %s", label, error)
        return False, error
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


@dataclass
class _SideProbe:
    """Results of the probes that only touch one side (src or dst)."""

    side: str
    tcp: bool = False
    query: bool = False
    owner_query: bool = False
    pool: Pool | None = None
    errors: dict[str, str] = field(default_factory=dict)


async def _probe_side(side: str, db_conf: DbConfig, logger: Logger) -> _SideProbe:
    """Run the TCP, root and owner probes for one side.

    Every failure is caught and recorded in ``errors`` under the failed check's
    name, so a failure on one side never stops the other side's probes.
    On success the root pool is left open on ``pool`` for the dblink checks;
    the caller closes it.
    """
    probe = _SideProbe(side=side)

    probe.tcp, tcp_error = await _check_tcp(db_conf.ip, db_conf.port, logger)
    if not probe.tcp:
        probe.errors[f"{side}_tcp"] = tcp_error or "tcp check failed"
        return probe

    try:
        probe.pool = await create_pool(db_conf.root_uri, min_size=1)
    except Exception as e:
        error = f"root connection failed: {_format_error(e)}"
        logger.error(error)
        probe.errors[f"{side}_query"] = error
        return probe

    # SELECT 1 to validate credentials from pgbelt's perspective
    try:
        await probe.pool.fetchval("SELECT 1")
        probe.query = True
        logger.debug("SELECT 1 succeeded.")
    except Exception as e:
        error = f"root SELECT 1 failed: {_format_error(e)}"
        logger.error(error)
        probe.errors[f"{side}_query"] = error
        return probe

    # Owner-credential probe -- gated on the matching root SELECT 1
    # because owner sits behind the same network path; if root can't
    # query, owner won't either, and we'd rather surface the underlying
    # network/RDS problem on the ``*_query`` cell than double-flag it.
    probe.owner_query, owner_error = await _check_owner_query(
        db_conf.owner_uri, f"{side} owner", logger
    )
    if not probe.owner_query:
        probe.errors[f"{side}_owner_query"] = owner_error or "owner check failed"

    return probe


async def _check_dblink(
    pool: Pool, remote_dsn: str, logger: Logger
) -> tuple[bool, str | None]:
    """Create dblink if needed, then connect from this server to the remote one."""
    try:
        await ensure_dblink(pool, logger)
    except Exception as e:
        error = f"could not create the dblink extension: {_format_error(e)}"
        logger.error(error)
        return False, error
    return await check_connectivity_via_dblink(pool, remote_dsn, logger)


def _fill_skipped(checks: dict[str, bool], errors: dict[str, str]) -> None:
    """Give every failed check without its own error a "skipped" reason, so
    operators can tell a real failure from a probe that never ran."""
    prerequisites = {
        "src_query": ["src_tcp"],
        "src_owner_query": ["src_query"],
        "dst_query": ["dst_tcp"],
        "dst_owner_query": ["dst_query"],
        "src_to_dst_dblink": ["src_query", "dst_query"],
        "dst_to_src_dblink": ["dst_query", "src_query"],
    }
    for check, required in prerequisites.items():
        if checks[check] or check in errors:
            continue
        failed = [r for r in required if not checks[r]]
        if failed:
            errors[check] = f"skipped: {' and '.join(failed)} failed"


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

    Each side is probed independently. If one side rejects the connection
    (for example, no pg_hba.conf entry), the other side's checks still run,
    and the error text is reported per check, along with each side's host.

    The dblink extension is created if not present and left in place. It is
    removed by belt teardown --full.

    If any check fails, the command will exit 1. It will test ALL connections
    before returning exit code 1 or 0, and output which checks passed/failed.
    """

    conf = await config_future

    src_logger = get_logger(conf.db, conf.dc, "connect.src")
    dst_logger = get_logger(conf.db, conf.dc, "connect.dst")

    src, dst = await gather(
        _probe_side("src", conf.src, src_logger),
        _probe_side("dst", conf.dst, dst_logger),
    )

    errors: dict[str, str] = {**src.errors, **dst.errors}
    src_to_dst_dblink = False
    dst_to_src_dblink = False

    try:
        # dblink cross-host checks
        if src.query and dst.query:
            src_to_dst_dblink, src_to_dst_error = await _check_dblink(
                src.pool, conf.dst.root_dsn, src_logger
            )
            if not src_to_dst_dblink:
                errors["src_to_dst_dblink"] = src_to_dst_error or "dblink failed"
            dst_to_src_dblink, dst_to_src_error = await _check_dblink(
                dst.pool, conf.src.root_dsn, dst_logger
            )
            if not dst_to_src_dblink:
                errors["dst_to_src_dblink"] = dst_to_src_error or "dblink failed"
    finally:
        await gather(*[p.pool.close() for p in (src, dst) if p.pool is not None])

    checks = {
        "src_tcp": src.tcp,
        "src_query": src.query,
        "src_owner_query": src.owner_query,
        "src_to_dst_dblink": src_to_dst_dblink,
        "dst_tcp": dst.tcp,
        "dst_query": dst.query,
        "dst_owner_query": dst.owner_query,
        "dst_to_src_dblink": dst_to_src_dblink,
    }
    _fill_skipped(checks, errors)

    return {
        "db": conf.db,
        **checks,
        "src_host": conf.src.host,
        "dst_host": conf.dst.host,
        "errors": errors,
    }


COMMANDS = [src_dsn, dst_dsn, check_pkeys, check_connectivity]
