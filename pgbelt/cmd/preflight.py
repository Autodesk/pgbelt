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


def _summary_table(results: dict, compared_extensions: list[str] = None) -> list[list]:
    """
    Takes a dict of precheck results for all databases and returns a summary table for echo.

    The summary table alters slightly if the results are for a destination database.

    results format:
    [
        {
            "server_version": "9.6.20",
            "max_replication_slots": "10",
            "max_worker_processes": "10",
            "max_wal_senders": "10",
            "shared_preload_libraries": ["pg_stat_statements", ...],
            "rds.logical_replication": "on",
            "schema: "public",
            "extensions": ["uuid-ossp", ...],
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
            style("shared_preload_libraries", "yellow"),
            style("rds.logical_replication", "yellow"),
            style("root user ok", "yellow"),
            style("owner user ok", "yellow"),
            style("targeted schema", "yellow"),
            style("extensions ok", "yellow"),
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

        # Interestingly enough, we can tell if this is being run for a destination database if the compared_extensions is not None.
        # This is because it is only set when we are ensuring all source extensions are in the destination.
        is_dest_db = compared_extensions is not None

        # If this is a destination database, we need to check if the owner can create objects.

        if is_dest_db:
            owner_ok = (r["users"]["owner"]["rolcanlogin"]) and (
                r["users"]["owner"]["can_create"]
            )
        else:
            owner_ok = r["users"]["owner"]["rolcanlogin"]

        shared_preload_libraries = "ok"
        missing = []
        if "pg_stat_statements" not in r["shared_preload_libraries"]:
            missing.append("pg_stat_statements")
        if "pglogical" not in r["shared_preload_libraries"]:
            missing.append("pglogical")
        if missing:
            shared_preload_libraries = ", ".join(missing) + " are missing!"

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
                    shared_preload_libraries,
                    "green" if shared_preload_libraries == "ok" else "red",
                ),
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

        # If this is a destinatino DB, we are ensuring all source extensions are in the destination.
        # If not, we don't want this column in the table.
        if is_dest_db:
            extensions_ok = all(
                [e in r["extensions"] for e in compared_extensions]
            ) and all([e in compared_extensions for e in r["extensions"]])
            summary_table[-1].append(
                style(extensions_ok, "green" if extensions_ok else "red")
            )

    return summary_table


def _users_table(users: dict, is_dest_db: bool = False) -> list[list]:
    """
    Takes a dict of user info and returns a table of the users for echo.

    The users table alters slightly if the results are for a destination database.

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

    if is_dest_db:
        users_table[0].insert(
            5, style("can create objects in targeted schema", "yellow")
        )

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
            style("not required", "green"),
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
            style(
                users["owner"]["can_create"],
                "green" if users["owner"]["can_create"] else "red",
            ),
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
                style(s["Schema"], "green" if s["Schema"] == schema_name else "red"),
                style(s["Owner"], "green" if s["Owner"] == owner_name else "red"),
            ]
        )

    return sequences_table


def _extensions_table(
    source_extensions: list[str], destination_extensions: list[str]
) -> list[list]:
    """

    Takes a list of source and destination extensions and returns a table of the extensions for echo.
    It will flag any extensions that are not in the destination database but are in the source database.

    <source/destination>_extensions format:
    [
        "uuid-ossp",
        ...
    ]

    """

    extensions_table = [
        [
            style("extension in source DB", "yellow"),
            style("is in destination", "yellow"),
        ]
    ]

    for e in source_extensions:
        extensions_table.append(
            [
                style(e["extname"], "green"),
                style(
                    e in destination_extensions,
                    "green" if e in destination_extensions else "red",
                ),
            ]
        )

    return extensions_table


async def _print_prechecks(results: list[dict]) -> list[list]:
    """
    Print out the results of the prechecks in a human readable format.
    If there are multiple databases, only print the summary table.
    If there is only one database, print the summary table and more detailed info.

    results format:
    [
        {
            "db": "db_name",
            "src": {
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
                        "memberof": ["rds_superuser", ...],
                        "can_create": True
                    }
                }
            },
            "dst": {
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
                        "memberof": ["rds_superuser", ...],
                        "can_create": True
                    }
                }
            }
        },
        ...
    ]
    """

    src_summaries = []
    dst_summaries = []
    for r in results:
        src_summaries.append(r["src"])
        dst_summaries.append(r["dst"])

    src_summary_table = _summary_table(src_summaries)
    dst_summary_table = _summary_table(
        dst_summaries, compared_extensions=r["src"]["extensions"]
    )

    if len(results) != 1:

        # For mulitple databases, we only print the summary table.

        src_multi_display_string = (
            style("\nSource DB Configuration Summary", "blue")
            + "\n"
            + tabulate(src_summary_table, headers="firstrow")
        )
        echo(src_multi_display_string)
        dst_multi_display_string = (
            style("\nDestination DB Configuration Summary", "blue")
            + "\n"
            + tabulate(dst_summary_table, headers="firstrow")
        )
        echo(dst_multi_display_string)

        return src_multi_display_string, dst_multi_display_string

    # If we ran only on one db print more detailed info
    r = results[0]

    # TODO: We should confirm the named schema exists in the database and alert the user if it does not (red in column if not found).

    # Source DB Tables

    src_users_table = _users_table(r["src"]["users"])
    src_tables_table = _tables_table(
        r["src"]["tables"],
        r["src"]["pkeys"],
        r["src"]["users"]["owner"]["rolname"],
        r["src"]["schema"],
    )
    src_sequences_table = _sequences_table(
        r["src"]["sequences"], r["src"]["users"]["owner"]["rolname"], r["src"]["schema"]
    )

    if len(src_tables_table) == 1:
        src_tables_table = [
            [
                style(
                    "ALERT: Not able to find tables to replicate, check your config's 'schema_name'",
                    "red",
                )
            ]
        ]

    if len(src_sequences_table) == 1:
        src_sequences_table = [
            [
                style(
                    "ALERT: Not able to find sequences to replicate, check your config's 'schema_name'",
                    "red",
                )
            ]
        ]

    source_display_string = (
        style("\nSource DB Configuration Summary", "blue")
        + "\n"
        + "\n"
        + tabulate(src_summary_table, headers="firstrow")
        + "\n"
        + style("\nRequired Users Summary", "yellow")
        + "\n"
        + tabulate(src_users_table, headers="firstrow")
        + "\n"
        + style("\nTable Compatibility Summary", "yellow")
        + "\n"
        + tabulate(
            src_tables_table, headers="firstrow" if len(src_tables_table) > 1 else ""
        )
        + "\n"
        + style("\nSequence Compatibility Summary", "yellow")
        + "\n"
        + tabulate(
            src_sequences_table,
            headers="firstrow" if len(src_sequences_table) > 1 else "",
        )
    )

    echo(source_display_string)

    echo("\n" + "=" * 80)

    # Destination DB Tables

    dst_users_table = _users_table(r["dst"]["users"], is_dest_db=True)
    extenstions_table = _extensions_table(
        r["src"]["extensions"], r["dst"]["extensions"]
    )

    destination_display_string = (
        style("\nDestination DB Configuration Summary", "blue")
        + "\n"
        + "\n"
        + tabulate(dst_summary_table, headers="firstrow")
        + "\n"
        + style("\nExtension Matchup Summary", "yellow")
        + "\n"
        + tabulate(extenstions_table, headers="firstrow")
        + "\n"
        + style("\nRequired Users Summary", "yellow")
        + "\n"
        + tabulate(dst_users_table, headers="firstrow")
    )

    echo(destination_display_string)

    return src_summary_table, dst_summary_table


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

    conf = await config_future
    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.src.owner_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_root_pool, src_owner_pool, dst_root_pool = pools

    try:
        src_logger = get_logger(conf.db, conf.dc, "preflight.src")
        dst_logger = get_logger(conf.db, conf.dc, "preflight.dst")

        result = {}

        # Source DB Data
        result["src"] = await precheck_info(
            src_root_pool,
            conf.src.root_user.name,
            conf.src.owner_user.name,
            conf.tables,
            conf.sequences,
            conf.schema_name,
            src_logger,
        )
        result["src"]["pkeys"], _, _ = await analyze_table_pkeys(
            src_owner_pool, conf.schema_name, src_logger
        )
        result["src"]["schema"] = conf.schema_name

        # Destination DB Data
        result["dst"] = await precheck_info(
            dst_root_pool,
            conf.dst.root_user.name,
            conf.dst.owner_user.name,
            conf.tables,
            conf.sequences,
            conf.schema_name,
            dst_logger,
        )
        # No need to analyze pkeys for the destination database (we use this to determine replication method in only the forward case).
        result["dst"]["schema"] = conf.schema_name

        # The precheck view code treats "db" as the name of the database pair, not the logical dbname of the database.
        result["src"]["db"] = conf.db
        result["dst"]["db"] = conf.db

        return result
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [precheck]
