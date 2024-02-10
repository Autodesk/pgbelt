from asyncio import gather
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.postgres import precheck_info
from tabulate import tabulate
from typer import echo
from typer import style


async def _print_prechecks(results: list[dict]) -> list[list]:
    summary_table = [
        [
            style("database", "yellow"),
            style("server_version", "yellow"),
            style("max_replication_slots", "yellow"),
            style("max_worker_processes", "yellow"),
            style("max_wal_senders", "yellow"),
            style("pg_stat_statements", "yellow"),
            style("pglogical", "yellow"),
            style("rds.logical_replication", "yellow"),
            style("root user ok", "yellow"),
            style("owner user ok", "yellow"),
        ]
    ]

    results.sort(key=lambda d: d["db"])

    for r in results:
        root_ok = (
            r["root"]["rolcanlogin"]
            and r["root"]["rolcreaterole"]
            and r["root"]["rolinherit"]
        ) and ("rds_superuser" in r["root"]["memberof"] or r["root"]["rolsuper"])
        # TODO: New check - the config owner must be able to create objects in the named schema in the target database.
        # Might be okay to check on both ends instead of checking just on the target...?
        owner_ok = r["owner"]["rolcanlogin"]
        pg_stat_statements = (
            "installed"
            if "pg_stat_statements" in r["shared_preload_libraries"]
            else "not installed"
        )
        pglogical = (
            "installed"
            if "pglogical" in r["shared_preload_libraries"]
            else "not installed"
        )
        summary_table.append(
            [
                style(r["db"], "green"),
                style(
                    r["server_version"],
                    (
                        "green"
                        if float(
                            r["server_version"].rsplit(" ", 1)[0].rsplit(".", 1)[0]
                        )
                        >= 9.6
                        else "red"
                    ),
                ),
                style(
                    r["max_replication_slots"],
                    "green" if int(r["max_replication_slots"]) >= 2 else "red",
                ),
                style(
                    r["max_worker_processes"],
                    "green" if int(r["max_worker_processes"]) >= 2 else "red",
                ),
                style(
                    r["max_wal_senders"],
                    "green" if int(r["max_wal_senders"]) >= 10 else "red",
                ),
                style(
                    pg_stat_statements,
                    "green" if pg_stat_statements == "installed" else "red",
                ),
                style(pglogical, "green" if pglogical == "installed" else "red"),
                style(
                    r["rds.logical_replication"],
                    (
                        "green"
                        if r["rds.logical_replication"] in ["on", "Not Applicable"]
                        else "red"
                    ),
                ),
                style(root_ok, "green" if root_ok else "red"),
                style(owner_ok, "green" if owner_ok else "red"),
            ]
        )

    if len(results) != 1:
        return summary_table

    # If we ran only on one db print more detailed info
    r = results[0]
    users_table = [
        [
            style("user", "yellow"),
            style("name", "yellow"),
            style("can log in", "yellow"),
            style("can make roles", "yellow"),
            style("is superuser", "yellow"),
        ]
    ]

    root_in_superusers = (
        "rds_superuser" in r["root"]["memberof"] and r["root"]["rolinherit"]
    ) or (r["root"]["rolsuper"])

    users_table.append(
        [
            style("root", "green"),
            style(r["root_name"], "green"),
            style(
                r["root"]["rolcanlogin"], "green" if r["root"]["rolcanlogin"] else "red"
            ),
            style(
                r["root"]["rolcreaterole"],
                "green" if r["root"]["rolcreaterole"] else "red",
            ),
            style(root_in_superusers, "green" if root_in_superusers else "red"),
        ]
    )

    users_table.append(
        [
            style("owner", "green"),
            style(r["owner_name"], "green"),
            style(
                r["owner"]["rolcanlogin"],
                "green" if r["owner"]["rolcanlogin"] else "red",
            ),
            style("not required", "green"),
            style("not required", "green"),
        ]
    )

    tables_table = [
        [
            style("table name", "yellow"),
            style("can replicate", "yellow"),
            style("replication type", "yellow"),
            style("schema", "yellow"),
            style("owner", "yellow"),
        ]
    ]

    # TODO: Since we are now targeting tables and sequences in a named schema (from the config),
    # we can drop the Schema column and instead add a schema column to the database.
    # TODO: We should confirm the named schema exists in the database and alert the user if it does not (red in column if not found).

    for t in r["tables"]:
        # TODO: The tables that show up here will be the ones that are in the named schema in the config.
        # Due to this, if no tables show up, it's likely that the schema name is incorrect. We need to alert the user to this.
        can_replicate = t["Schema"] == "public" and t["Owner"] == r["owner_name"]
        replication = (
            ("pglogical" if t["Name"] in r["pkeys"] else "dump and load")
            if can_replicate
            else "unavailable"
        )
        tables_table.append(
            [
                style(t["Name"], "green"),
                style(can_replicate, "green" if can_replicate else "red"),
                style(replication, "green" if can_replicate else "red"),
                style(
                    t["Schema"], "green" if t["Schema"] == "public" else "red"
                ),  # TODO: This is key. Ensure the table's owner matches the owner in the config.
                style(t["Owner"], "green" if t["Owner"] == r["owner_name"] else "red"),
            ]
        )

    sequences_table = [
        [
            style("sequence name", "yellow"),
            style("can replicate", "yellow"),
            style("schema", "yellow"),
            style("owner", "yellow"),
        ]
    ]

    for s in r["sequences"]:
        # TODO: The sequences that show up here will be the ones that are in the named schema in the config.
        # Due to this, if no sequences show up, it's likely that the schema name is incorrect. We need to alert the user to this.
        can_replicate = s["Schema"] == "public" and s["Owner"] == r["owner_name"]
        sequences_table.append(
            [
                style(s["Name"], "green"),
                style(can_replicate, "green" if can_replicate else "red"),
                style(
                    s["Schema"], "green" if s["Schema"] == "public" else "red"
                ),  # TODO: This is key. Ensure the sequence's owner matches the owner in the config.
                style(s["Owner"], "green" if s["Owner"] == r["owner_name"] else "red"),
            ]
        )

    display_string = (
        style("\nDB Configuration Summary", "yellow")
        + "\n"
        + tabulate(summary_table, headers="firstrow")
        + "\n"
        + style("\nRequired Users Summary", "yellow")
        + "\n"
        + tabulate(users_table, headers="firstrow")
        + "\n"
        + style("\nTable Compatibility Summary", "yellow")
        + "\n"
        + tabulate(tables_table, headers="firstrow")
        + "\n"
        + style("\nSequence Compatibility Summary", "yellow")
        + "\n"
        + tabulate(sequences_table, headers="firstrow")
    )

    echo(display_string)

    return summary_table


@run_with_configs(skip_dst=True, results_callback=_print_prechecks)
async def precheck(config_future: Awaitable[DbupgradeConfig]) -> dict:
    """
    Report whether your source database meets the basic requirements for pgbelt.
    Any red item in a row in the table indicates a requirement not satisfied by your db.
    This command can not check network connectivity between your source and destination!

    If a dbname is given this will also show whether the configuration of
    the root and owner users seems ok and a summary of whether each
    table and sequence in the database can be replicated.
    If a row contains any red that sequence or table can not be replicated.
    """

    # TODO: This entire precheck only checks the source database. We have to redesign this to check both the source and destination.
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.src.owner_uri, min_size=1),
    )
    root_pool, owner_pool = pools

    try:
        src_logger = get_logger(conf.db, conf.dc, "preflight.src")
        result = await precheck_info(
            root_pool, conf.src.root_user.name, conf.src.owner_user.name, src_logger
        )
        result["db"] = conf.db
        result["root_name"] = conf.src.root_user.name
        result["owner_name"] = conf.src.owner_user.name
        result["pkeys"], _, _ = await analyze_table_pkeys(
            owner_pool, conf.src.schema, src_logger
        )
        return result
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [precheck]
