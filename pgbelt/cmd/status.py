from asyncio import gather
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util import get_logger
from pgbelt.util.pglogical import dst_status
from pgbelt.util.pglogical import src_status
from pgbelt.util.postgres import initialization_progress
from pgbelt.util.postgres import analyze_table_pkeys
from rich.console import Console
from rich.table import Table


async def _print_status_table(results: list[dict[str, str]]) -> list[list[str]]:

    table = Table(title="Replication Status")
    table.add_column("Database")
    table.add_column("SRC -> DST")
    table.add_column("SRC <- DST")
    table.add_column("sent_lag")
    table.add_column("flush_lag")
    table.add_column("write_lag")
    table.add_column("replay_lag")
    table.add_column("SRC Dataset Size")
    table.add_column("DST Dataset Size")
    table.add_column("Progress")

    results.sort(key=lambda d: d["db"])

    for r in results:

        table.add_row(
            r["db"],
            (
                "[green]" + r["pg1_pg2"]
                if r["pg1_pg2"] == "replicating"
                else "[red]" + r["pg1_pg2"]
            ),
            (
                "[green]" + r["pg2_pg1"]
                if r["pg2_pg1"] == "replicating"
                else "[red]" + r["pg2_pg1"]
            ),
            (
                "[green]" + r["sent_lag"]
                if r["sent_lag"] == "0"
                else "[red]" + r["sent_lag"]
            ),
            (
                "[green]" + r["flush_lag"]
                if r["flush_lag"] == "0"
                else "[red]" + r["flush_lag"]
            ),
            (
                "[green]" + r["write_lag"]
                if r["write_lag"] == "0"
                else "[red]" + r["write_lag"]
            ),
            (
                "[green]" + r["replay_lag"]
                if r["replay_lag"] == "0"
                else "[red]" + r["replay_lag"]
            ),
            "[green]" + r["src_dataset_size"],
            "[green]" + r["dst_dataset_size"],
            "[green]" + r["progress"],
        )

    console = Console()
    console.print(table)

    return table


@run_with_configs(results_callback=_print_status_table)
async def status(conf_future: Awaitable[DbupgradeConfig]) -> dict[str, str]:
    """
    Print out a table of status information for one or all of the dbs in a datacenter.
    Contains the pglogical replication status for both directions of replication and
    replication lag data for forward replication. Possible replication statuses are as
    follows:

    unconfigured: No replication has been set up in this direction yet.

    initializing: Pglogical is performing an initial data dump to bring the follower up to speed.
    You can not begin replication in the opposite direction during this stage.

    replicating: Pglogical is replicating only net new writes in this direction.

    down: Pglogical has encountered an error and has stopped replicating entirely.
    Check the postgres logs on both dbs to determine the cause.
    """
    conf = await conf_future
    src_logger = get_logger(conf.db, conf.dc, "status.src")
    dst_logger = get_logger(conf.db, conf.dc, "status.dst")

    pools = await gather(
        create_pool(dsn=conf.src.root_uri, min_size=1),
        create_pool(dsn=conf.dst.root_uri, min_size=1),
    )
    src_pool, dst_pool = pools

    # Get the list of targeted tables by first getting all tables, then filtering whatever is in the config.
    pkey_tables, non_pkey_tables, _ = await analyze_table_pkeys(
        src_pool, conf.schema_name, src_logger
    )
    all_tables = pkey_tables + non_pkey_tables
    target_tables = all_tables
    if conf.tables:
        target_tables = [t for t in all_tables if t in conf.tables]

    if not target_tables:
        raise ValueError(
            f"Targeted tables not found in the source database. Please check your config's schema and tables. DB: {conf.db} DC: {conf.dc}, SCHEMA: {conf.schema_name} TABLES: {conf.tables}."
        )

    try:
        result = await gather(
            src_status(src_pool, src_logger),
            dst_status(dst_pool, dst_logger),
            initialization_progress(
                target_tables,
                conf.schema_name,
                conf.schema_name,
                src_pool,
                dst_pool,
                src_logger,
                dst_logger,
            ),
        )

        result[0].update(result[1])
        result[0]["db"] = conf.db

        # We should hide the progress in the following cases:
        # 1. When src -> dst is replicating and dst -> src is any state (replicating, unconfigured, down)
        #    a. We do this because the size when done still will be a tad smaller than SRC, showing <100%
        # 2. When src -> dst is unconfigured and dst -> src is replicating (not down or unconfigured)
        #    a. We do this because reverse-only occurs at the start of cutover and onwards, and seeing the progress at that stage is not useful.
        if (result[0]["pg1_pg2"] == "replicating") or (  # 1
            result[0]["pg1_pg2"] == "unconfigured"
            and result[0]["pg2_pg1"] == "replicating"
        ):  # 2
            result[2]["src_dataset_size"] = "n/a"
            result[2]["dst_dataset_size"] = "n/a"
            result[2]["progress"] = "n/a"

        result[0].update(result[2])
        return result[0]
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [status]
