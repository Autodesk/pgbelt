import copy
from asyncio import gather
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.postgres import precheck_info
from rich.console import Console
from pgbelt.util.rich import RichTableArgs, build_rich_table


def _summary_table(
    results: dict, title_str: str, compared_extensions: list[str] = None
) -> RichTableArgs:
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

    rich_table_args = RichTableArgs(
        title=title_str,
        columns=[
            "Database",
            "Server Version",
            "max_replication_slots",
            "max_worker_processes",
            "max_wal_senders",
            "shared_preload_libraries",
            "rds.logical_replication",
            "Root User OK",
            "Owner User OK",
            "Targeted Schema",
        ],
        rows=[],
    )

    # Interestingly enough, we can tell if this is being run for a destination database if the compared_extensions is not None.
    # This is because it is only set when we are ensuring all source extensions are in the destination.
    is_dest_db = compared_extensions is not None
    if is_dest_db:
        existing_columns = copy.deepcopy(rich_table_args.columns)
        existing_columns.append("Extensions OK")
        rich_table_args.columns = existing_columns

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

        rich_table_args.rows.append(
            [
                r["db"],
                (
                    "[green]" + r["server_version"]
                    if float(r["server_version"].rsplit(" ", 1)[0].rsplit(".", 1)[0])
                    >= 9.6
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
            ]
        )

    return rich_table_args


def _users_table(users: dict, is_dest_db: bool = False) -> RichTableArgs:
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

    rich_table_args = RichTableArgs(
        title="Required Users Summary",
        columns=["User", "Name", "Can Log In", "Can Make Roles", "Is Superuser"],
        rows=[],
    )

    if is_dest_db:
        existing_columns = copy.deepcopy(rich_table_args.columns)
        existing_columns.append("Can Create Objects")
        rich_table_args.columns = existing_columns

    root_in_superusers = (
        "rds_superuser" in users["root"]["memberof"] and users["root"]["rolinherit"]
    ) or (users["root"]["rolsuper"])

    rich_table_args.rows.append(
        [
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
        ]
    )

    rich_table_args.rows.append(
        [
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
        ]
    )

    return rich_table_args


def _tables_table(
    tables: list[dict], pkeys: list[dict], owner_name: str, schema_name: str
) -> RichTableArgs:
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

    rich_table_args = RichTableArgs(
        title="Table Compatibility Summary",
        columns=["Table Name", "Can Replicate", "Replication Type", "Schema", "Owner"],
        rows=[],
    )

    for t in tables:
        can_replicate = t["Schema"] == schema_name and t["Owner"] == owner_name
        replication = (
            ("pglogical" if t["Name"] in pkeys else "dump and load")
            if can_replicate
            else "unavailable"
        )

        rich_table_args.rows.append(
            [
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
            ]
        )

    return rich_table_args


def _sequences_table(
    sequences: list[dict], owner_name: str, schema_name: str
) -> RichTableArgs:
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

    rich_table_args = RichTableArgs(
        title="Sequence Compatibility Summary",
        columns=["Sequence Name", "Can Replicate", "Schema", "Owner"],
        rows=[],
    )

    for s in sequences:
        can_replicate = s["Schema"] == schema_name and s["Owner"] == owner_name

        rich_table_args.rows.append(
            [
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
            ]
        )

    return rich_table_args


def _extensions_table(
    source_extensions: list[str], destination_extensions: list[str]
) -> RichTableArgs:
    """

    Takes a list of source and destination extensions and returns a table of the extensions for echo.
    It will flag any extensions that are not in the destination database but are in the source database.

    <source/destination>_extensions format:
    [
        "uuid-ossp",
        ...
    ]

    """

    rich_table_args = RichTableArgs(
        title="Extension Compatibility Summary",
        columns=["Extension Name in Source DB", "Is in Destination"],
        rows=[],
    )

    for e in source_extensions:
        rich_table_args.rows.append(
            [
                e["extname"],
                (
                    "[green]" + "True"
                    if e in destination_extensions
                    else "[red]" + "False"
                ),
            ]
        )

    return rich_table_args


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

    console = Console()
    src_summary_rich_table = build_rich_table(src_summary_table)
    dst_summary_rich_table = build_rich_table(dst_summary_table)

    if len(results) != 1:
        # For mulitple databases, we only print the summary table.
        console.print(src_summary_rich_table)
        console.print(dst_summary_rich_table)

        return src_summary_table, dst_summary_table

    # If we ran only on one db print more detailed info
    r = results[0]

    # TODO: We should confirm the named schema exists in the database and alert the user if it does not (red in column if not found).

    # Source DB Tables

    src_users_table = _users_table(r["src"]["users"])
    src_users_rich_table = build_rich_table(src_users_table)

    src_tables_table = _tables_table(
        r["src"]["tables"],
        r["src"]["pkeys"],
        r["src"]["users"]["owner"]["rolname"],
        r["src"]["schema"],
    )
    src_tables_rich_table = build_rich_table(src_tables_table)

    src_sequences_table = _sequences_table(
        r["src"]["sequences"], r["src"]["users"]["owner"]["rolname"], r["src"]["schema"]
    )
    src_sequences_rich_table = build_rich_table(src_sequences_table)

    if len(r["src"]["tables"]) < 1:
        src_tables_rich_table = "[red]ALERT: Not able to find tables to replicate, check your config's 'schema_name'"

    if len(r["src"]["sequences"]) < 1:
        src_sequences_rich_table = "[red]ALERT: Not able to find sequences to replicate, check your config's 'schema_name'"

    console = Console()
    console.print("\n\n")
    console.print(src_summary_rich_table)
    console.print("\n")
    console.print(src_users_rich_table)
    console.print("\n")
    console.print(src_tables_rich_table)
    console.print("\n")
    console.print(src_sequences_rich_table)

    console.print("\n" + "=" * 80)

    # Destination DB Tables

    dst_users_table = _users_table(r["dst"]["users"], is_dest_db=True)
    dst_users_rich_table = build_rich_table(dst_users_table)

    extenstions_table = _extensions_table(
        r["src"]["extensions"], r["dst"]["extensions"]
    )
    extenstions_rich_table = build_rich_table(extenstions_table)

    console.print("\n\n")
    console.print(dst_summary_rich_table)
    console.print("\n")
    console.print(extenstions_rich_table)
    console.print("\n")
    console.print(dst_users_rich_table)

    return (
        src_summary_table,
        src_users_table,
        src_tables_table,
        src_sequences_table,
        dst_summary_table,
        extenstions_table,
        dst_users_table,
    )


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
