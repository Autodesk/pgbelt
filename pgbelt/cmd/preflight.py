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


def _summary_table(results: dict) -> list[list]:
    """
    Takes a dict of precheck results for all databases and returns a summary table for echo.

    results format:
    [
        {
            "db": "db_name",
            "server_version": "9.6.20",
            "max_replication_slots": "10",
            "max_worker_processes": "10",
            "max_wal_senders": "10",
            "pg_stat_statements": "installed",
            "pglogical": "installed",
            "rds.logical_replication": "on",
            "schema: "public",
            "users": { // See pgbelt.util.postgres.precheck_info results["users"] for more info.
                "root": {
                    "rolname": "root",
                    "rolcanlogin": True,
                    "rolcreaterole": True,
                    "rolinherit": True,
                    "rolsuper": True,
                    "memberof": ["rds_superuser", ...]
                },
                "owner": {
                    "rolname": "owner",
                    "rolcanlogin": True,
                    "rolcreaterole": False,
                    "rolinherit": True,
                    "rolsuper": False,
                    "memberof": ["rds_superuser", ...]
                }
            }
        },
        ...
    ]
    """

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
            style("targeted schema", "yellow"),
        ]
    ]

    results.sort(key=lambda d: d["db"])

    for r in results:
        root_ok = (
            r["users"]["root"]["rolcanlogin"]
            and r["users"]["root"]["rolcreaterole"]
            and r["users"]["root"]["rolinherit"]
        ) and (
            "rds_superuser" in r["users"]["root"]["memberof"]
            or r["users"]["root"]["rolsuper"]
        )
        # TODO: New check - the config owner must be able to create objects in the named schema in the target database.
        # Might be okay to check on both ends instead of checking just on the target...?
        owner_ok = r["users"]["owner"]["rolcanlogin"]
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
                style(r["schema"], "green"),
            ]
        )

    return summary_table


def _users_table(users: dict) -> list[list]:
    """
    Takes a dict of user info and returns a table of the users for echo.

    users format:
    {
        "root": {
            "rolname": "root",
            "rolcanlogin": True,
            "rolcreaterole": True,
            "rolinherit": True,
            "rolsuper": True,
            "memberof": ["rds_superuser", ...]
        },
        "owner": {
            "rolname": "owner",
            "rolcanlogin": True,
            "rolcreaterole": False,
            "rolinherit": True,
            "rolsuper": False,
            "memberof": ["rds_superuser", ...]
        }
    }

    See pgbelt.util.postgres.precheck_info results["users"] for more info..
    """

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
        "rds_superuser" in users["root"]["memberof"] and users["root"]["rolinherit"]
    ) or (users["root"]["rolsuper"])

    users_table.append(
        [
            style("root", "green"),
            style(users["root"]["rolname"], "green"),
            style(
                users["root"]["rolcanlogin"],
                "green" if users["root"]["rolcanlogin"] else "red",
            ),
            style(
                users["root"]["rolcreaterole"],
                "green" if users["root"]["rolcreaterole"] else "red",
            ),
            style(root_in_superusers, "green" if root_in_superusers else "red"),
        ]
    )

    users_table.append(
        [
            style("owner", "green"),
            style(users["owner"]["rolname"], "green"),
            style(
                users["owner"]["rolcanlogin"],
                "green" if users["owner"]["rolcanlogin"] else "red",
            ),
            style("not required", "green"),
            style("not required", "green"),
        ]
    )

    return users_table


def _tables_table(
    tables: list[dict], pkeys: list[dict], owner_name: str, schema_name: str
) -> list[list]:
    """
    Takes a list of table dicts and returns a table of the tables for echo.

    tables format:
    [
        {
            "Name": "table_name",
            "Schema": "schema_name",
            "Owner": "owner_name"
        },
        ...
    ]
    """

    tables_table = [
        [
            style("table name", "yellow"),
            style("can replicate", "yellow"),
            style("replication type", "yellow"),
            style("schema", "yellow"),
            style("owner", "yellow"),
        ]
    ]

    for t in tables:
        can_replicate = t["Schema"] == schema_name and t["Owner"] == owner_name
        replication = (
            ("pglogical" if t["Name"] in pkeys else "dump and load")
            if can_replicate
            else "unavailable"
        )
        tables_table.append(
            [
                style(t["Name"], "green"),
                style(can_replicate, "green" if can_replicate else "red"),
                style(replication, "green" if can_replicate else "red"),
                style(t["Schema"], "green" if t["Schema"] == schema_name else "red"),
                style(t["Owner"], "green" if t["Owner"] == owner_name else "red"),
            ]
        )

    return tables_table


def _sequences_table(
    sequences: list[dict], owner_name: str, schema_name: str
) -> list[list]:
    """
    Takes a list of sequence dicts and returns a table of the sequences for echo.

    sequences format:
    [
        {
            "Name": "sequence_name",
            "Schema": "schema_name",
            "Owner": "owner_name"
        },
        ...
    ]
    """

    sequences_table = [
        [
            style("sequence name", "yellow"),
            style("can replicate", "yellow"),
            style("schema", "yellow"),
            style("owner", "yellow"),
        ]
    ]

    for s in sequences:
        can_replicate = s["Schema"] == schema_name and s["Owner"] == owner_name
        sequences_table.append(
            [
                style(s["Name"], "green"),
                style(can_replicate, "green" if can_replicate else "red"),
                style(
                    s["Schema"], "green" if s["Schema"] == schema_name else "red"
                ),  # TODO: This is key. Ensure the sequence's owner matches the owner in the config.
                style(s["Owner"], "green" if s["Owner"] == owner_name else "red"),
            ]
        )

    return sequences_table


async def _print_prechecks(results: list[dict]) -> list[list]:
    """
    Print out the results of the prechecks in a human readable format.
    If there are multiple databases, only print the summary table.
    If there is only one database, print the summary table and more detailed info.
    """

    summary_table = _summary_table(results)

    if len(results) != 1:

        # For mulitple databases, we only print the summary table.
        # TODO: Add a summary table for desination DBs.

        multi_display_string = (
            style("\nSource DB Configuration Summary", "yellow")
            + "\n"
            + tabulate(summary_table, headers="firstrow")
        )
        echo(multi_display_string)

        return multi_display_string

    # If we ran only on one db print more detailed info
    r = results[0]

    # TODO: Since we are now targeting tables and sequences in a named schema (from the config),
    # we can drop the Schema column and instead add a schema column to the database.
    # TODO: We should confirm the named schema exists in the database and alert the user if it does not (red in column if not found).

    users_table = _users_table(r["users"])
    tables_table = _tables_table(
        r["tables"], r["pkeys"], r["users"]["owner"]["rolname"], r["schema"]
    )
    sequences_table = _sequences_table(
        r["sequences"], r["users"]["owner"]["rolname"], r["schema"]
    )

    source_display_string = (
        style("\nSource DB Configuration Summary", "yellow")
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

    echo(source_display_string)

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
            root_pool,
            conf.src.root_user.name,
            conf.src.owner_user.name,
            conf.tables,
            conf.sequences,
            src_logger,
        )
        result["db"] = conf.db
        result["pkeys"], _, _ = await analyze_table_pkeys(
            owner_pool, conf.src.schema, src_logger
        )
        result["schema"] = conf.src.schema
        return result
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [precheck]
