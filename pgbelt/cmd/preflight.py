from asyncio import gather
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.postgres import precheck_info
from rich.console import Console
from rich.table import Table


def _summary_table(
    results: dict, title_str: str, compared_extensions: list[str] = None
) -> list[list]:
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

    table = Table(title=title_str)

    table.add_column("Database")
    table.add_column("Server Version")
    table.add_column("max_replication_slots")
    table.add_column("max_worker_processes")
    table.add_column("max_wal_senders")
    table.add_column("shared_preload_libraries")
    table.add_column("rds.logical_replication")
    table.add_column("Root User OK")
    table.add_column("Owner User OK")
    table.add_column("Targeted Schema")

    # Interestingly enough, we can tell if this is being run for a destination database if the compared_extensions is not None.
    # This is because it is only set when we are ensuring all source extensions are in the destination.
    is_dest_db = compared_extensions is not None
    if is_dest_db:
        table.add_column("Extensions OK")

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

        # If this is a destination DB, we are ensuring all source extensions are in the destination.
        # If not, we don't want this column in the table.
        extensions_ok = None
        if is_dest_db:
            extensions_ok = all(
                [e in r["extensions"] for e in compared_extensions]
            ) and all([e in compared_extensions for e in r["extensions"]])
            extensions_ok = (
                "[green]" + str(extensions_ok)
                if extensions_ok
                else "[red]" + str(extensions_ok)
            )

        table.add_row(
            r["db"],
            (
                "[green]" + r["server_version"]
                if float(r["server_version"].rsplit(" ", 1)[0].rsplit(".", 1)[0]) >= 9.6
                else "[red]" + r["server_version"]
            ),
            (
                "[green]" + r["max_replication_slots"]
                if int(r["max_replication_slots"]) >= 2
                else "[red]" + r["max_replication_slots"]
            ),
            (
                "[green]" + r["max_worker_processes"]
                if int(r["max_worker_processes"]) >= 2
                else "[red]" + r["max_worker_processes"]
            ),
            (
                "[green]" + r["max_wal_senders"]
                if int(r["max_wal_senders"]) >= 10
                else "[red]" + r["max_wal_senders"]
            ),
            (
                "[green]" + shared_preload_libraries
                if shared_preload_libraries == "ok"
                else "[red]" + shared_preload_libraries
            ),
            (
                "[green]" + r["rds.logical_replication"]
                if r["rds.logical_replication"] in ["on", "Not Applicable"]
                else "[red]" + r["rds.logical_replication"]
            ),
            "[green]" + str(root_ok) if root_ok else "[red]" + str(root_ok),
            "[green]" + str(owner_ok) if owner_ok else "[red]" + str(owner_ok),
            "[green]" + r["schema"],
            extensions_ok,
        )

    return table


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

    table = Table(title="Required Users Summary")

    table.add_column("User")
    table.add_column("Name")
    table.add_column("Can Log In")
    table.add_column("Can Make Roles")
    table.add_column("Is Superuser")

    if is_dest_db:
        table.add_column("Can Create Objects")

    root_in_superusers = (
        "rds_superuser" in users["root"]["memberof"] and users["root"]["rolinherit"]
    ) or (users["root"]["rolsuper"])

    table.add_row(
        "root",
        users["root"]["rolname"],
        (
            "[green]" + str(users["root"]["rolcanlogin"])
            if users["root"]["rolcanlogin"]
            else "[red]" + str(users["root"]["rolcanlogin"])
        ),
        (
            "[green]" + str(users["root"]["rolcreaterole"])
            if users["root"]["rolcreaterole"]
            else "[red]" + str(users["root"]["rolcreaterole"])
        ),
        (
            "[green]" + str(root_in_superusers)
            if root_in_superusers
            else "[red]" + str(root_in_superusers)
        ),
        "not required",
    )
    table.add_row(
        "owner",
        users["owner"]["rolname"],
        (
            "[green]" + str(users["owner"]["rolcanlogin"])
            if users["owner"]["rolcanlogin"]
            else "[red]" + str(users["owner"]["rolcanlogin"])
        ),
        "not required",
        "not required",
        (
            "[green]" + str(users["owner"]["can_create"])
            if users["owner"]["can_create"]
            else "[red]" + str(users["owner"]["can_create"])
        ),
    )

    return table


def _tables_table(
    tables: list[dict], pkeys: list[dict], owner_name: str, schema_name: str
) -> Table:
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

    table = Table(title="Table Compatibility Summary")

    table.add_column("Table Name")
    table.add_column("Can Replicate")
    table.add_column("Replication Type")
    table.add_column("Schema")
    table.add_column("Owner")

    for t in tables:
        can_replicate = t["Schema"] == schema_name and t["Owner"] == owner_name
        replication = (
            ("pglogical" if t["Name"] in pkeys else "dump and load")
            if can_replicate
            else "unavailable"
        )

        table.add_row(
            t["Name"],
            (
                "[green]" + str(can_replicate)
                if can_replicate
                else "[red]" + str(can_replicate)
            ),
            "[green]" + replication if can_replicate else "[red]" + replication,
            (
                "[green]" + t["Schema"]
                if t["Schema"] == schema_name
                else "[red]" + t["Schema"]
            ),
            (
                "[green]" + t["Owner"]
                if t["Owner"] == owner_name
                else "[red]" + t["Owner"]
            ),
        )

    return table


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

    table = Table(title="Sequence Compatibility Summary")

    table.add_column("Sequence Name")
    table.add_column("Can Replicate")
    table.add_column("Schema")
    table.add_column("Owner")

    for s in sequences:
        can_replicate = s["Schema"] == schema_name and s["Owner"] == owner_name

        table.add_row(
            s["Name"],
            (
                "[green]" + str(can_replicate)
                if can_replicate
                else "[red]" + str(can_replicate)
            ),
            (
                "[green]" + s["Schema"]
                if s["Schema"] == schema_name
                else "[red]" + s["Schema"]
            ),
            (
                "[green]" + s["Owner"]
                if s["Owner"] == owner_name
                else "[red]" + s["Owner"]
            ),
        )

    return table


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

    table = Table(title="Extension Compatibility Summary")

    table.add_column("Extension Name in Source DB")
    table.add_column("Is in Destination")

    for e in source_extensions:
        table.add_row(
            e["extname"],
            "[green]" + "True" if e in destination_extensions else "[red]" + "False",
        )

    return table


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

    src_summary_table = _summary_table(src_summaries, "Source DB Configuration Summary")
    dst_summary_table = _summary_table(
        dst_summaries,
        "Destination DB Configuration Summary",
        compared_extensions=r["src"]["extensions"],
    )

    if len(results) != 1:
        console = Console()
        # For mulitple databases, we only print the summary table.

        console.print(src_summary_table)
        console.print(dst_summary_table)

        return src_summary_table, dst_summary_table

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

    if len(r["src"]["tables"]) < 1:
        src_tables_table = "[red]ALERT: Not able to find tables to replicate, check your config's 'schema_name'"

    if len(r["src"]["sequences"]) < 1:
        src_sequences_table = "[red]ALERT: Not able to find sequences to replicate, check your config's 'schema_name'"

    console = Console()
    console.print("\n\n")
    console.print(src_summary_table)
    console.print("\n")
    console.print(src_users_table)
    console.print("\n")
    console.print(src_tables_table)
    console.print("\n")
    console.print(src_sequences_table)

    console.print("\n" + "=" * 80)

    # Destination DB Tables

    dst_users_table = _users_table(r["dst"]["users"], is_dest_db=True)
    extenstions_table = _extensions_table(
        r["src"]["extensions"], r["dst"]["extensions"]
    )

    console.print("\n\n")
    console.print(dst_summary_table)
    console.print("\n")
    console.print(extenstions_table)
    console.print("\n")
    console.print(dst_users_table)

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
