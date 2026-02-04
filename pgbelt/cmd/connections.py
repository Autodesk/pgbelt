from asyncio import gather
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util import get_logger
from pgbelt.util.postgres import get_active_connections
from tabulate import tabulate
from typer import echo
from typer import Option
from typer import style


async def _print_connections_table(results: list[dict[str, str]]) -> list[list[str]]:
    table = [
        [
            style("database", "yellow"),
            style("src_count", "yellow"),
            style("src_users", "yellow"),
            style("dst_count", "yellow"),
            style("dst_users", "yellow"),
        ]
    ]

    results.sort(key=lambda d: d["db"])

    for r in results:
        # Format usernames dict as "user: count, user: count, ..."
        src_users_str = (
            ", ".join([f"{u}: {c}" for u, c in r["src_usernames"].items()]) or "none"
        )
        dst_users_str = (
            ", ".join([f"{u}: {c}" for u, c in r["dst_usernames"].items()]) or "none"
        )

        table.append(
            [
                style(r["db"], "green"),
                style(
                    str(r["src_count"]), "green" if r["src_count"] == 0 else "yellow"
                ),
                style(src_users_str, "green" if r["src_count"] == 0 else "yellow"),
                style(str(r["dst_count"]), "green"),
                style(dst_users_str, "green"),
            ]
        )

    echo(tabulate(table, headers="firstrow"))

    return table


@run_with_configs(results_callback=_print_connections_table)
async def connections(
    conf_future: Awaitable[DbupgradeConfig],
    exclude_users: list[str] = Option(
        [],
        "--exclude-user",
        "-e",
        help="Additional usernames to exclude (can be repeated). Always excludes rdsadmin and postgres.",
    ),
    exclude_patterns: list[str] = Option(
        [],
        "--exclude-pattern",
        "-p",
        help="LIKE patterns to exclude usernames (e.g. '%%repuser%%'). Can be repeated.",
    ),
) -> dict[str, str]:
    """
    Print out a table showing active database connections for each database pair.
    Displays the connection count and list of connected usernames for both source
    and destination databases.

    Always excludes 'rdsadmin' and 'postgres' users from the count.
    Use --exclude-user to exclude additional specific usernames.
    Use --exclude-pattern to exclude usernames matching LIKE patterns (e.g. '%%repuser%%').

    Example:
        belt connections testdc --exclude-user datadog --exclude-pattern '%%repuser%%'
    """
    conf = await conf_future
    src_logger = get_logger(conf.db, conf.dc, "connections.src")
    dst_logger = get_logger(conf.db, conf.dc, "connections.dst")

    pools = await gather(
        create_pool(dsn=conf.src.root_uri, min_size=1),
        create_pool(dsn=conf.dst.root_uri, min_size=1),
    )
    src_pool, dst_pool = pools

    try:
        result = await gather(
            get_active_connections(
                src_pool,
                src_logger,
                exclude_users=list(exclude_users) if exclude_users else None,
                exclude_patterns=list(exclude_patterns) if exclude_patterns else None,
            ),
            get_active_connections(
                dst_pool,
                dst_logger,
                exclude_users=list(exclude_users) if exclude_users else None,
                exclude_patterns=list(exclude_patterns) if exclude_patterns else None,
            ),
        )

        src_connections, dst_connections = result

        return {
            "db": conf.db,
            "src_count": src_connections["count"],
            "src_usernames": src_connections["usernames"],
            "dst_count": dst_connections["count"],
            "dst_usernames": dst_connections["usernames"],
        }
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [connections]
