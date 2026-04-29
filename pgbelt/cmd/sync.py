from asyncio import gather
from collections.abc import Awaitable
from decimal import Decimal
from logging import Logger
from typing import Any

from asyncpg import create_pool
from asyncpg import Pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import apply_target_constraints
from pgbelt.util.dump import create_target_indexes
from pgbelt.util.dump import dump_and_load_tables
from pgbelt.util.dump import dump_and_load_tables_with_details
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.postgres import compare_100_random_rows
from pgbelt.util.postgres import compare_latest_100_rows
from pgbelt.util.postgres import compare_tables_without_pkeys
from pgbelt.util.postgres import detect_pk_sequences
from pgbelt.util.postgres import dump_sequences
from pgbelt.util.postgres import load_sequences
from pgbelt.util.postgres import run_analyze
from pgbelt.util.postgres import set_pk_sequences_from_data
from tabulate import tabulate
from typer import echo
from typer import Option
from typer import style


def _sequence_value_as_int(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return int(val)
    return int(val)


async def _sync_sequences(
    targeted_sequences: list[str],
    schema: str,
    src_pool: Pool,
    dst_pool: Pool,
    src_logger: Logger,
    dst_logger: Logger,
    stride: int | None = None,
) -> dict[str, Any]:

    pk_details: list[dict] = []
    non_pk_details: list[dict] = []

    pk_seqs = await detect_pk_sequences(
        dst_pool, targeted_sequences, schema, dst_logger
    )

    seq_vals = await dump_sequences(src_pool, targeted_sequences, schema, src_logger)
    src_logger.info(f"Total sequences to sync: {seq_vals.keys()}")

    non_pk_vals = {k: v for k, v in seq_vals.items() if k not in pk_seqs}
    src_logger.info(f"PK sequences: {list(pk_seqs.keys())}")
    src_logger.info(f"Non-PK sequences: {list(non_pk_vals.keys())}")

    if pk_seqs:
        await set_pk_sequences_from_data(dst_pool, pk_seqs, schema, dst_logger)
        for name in pk_seqs:
            pk_details.append(
                {
                    "name": name,
                    "synced": True,
                    "method": "pk_max",
                }
            )

    if non_pk_vals:
        original_vals = dict(non_pk_vals)

        if stride is not None:
            src_logger.info(
                f"Applying stride to non-PK sequences: source_value + {stride}"
            )
            non_pk_vals = {k: v + stride for k, v in non_pk_vals.items()}

        dst_current: dict[str, int] = {}
        for seq_name in non_pk_vals:
            val = await dst_pool.fetchval(
                f'SELECT last_value FROM {schema}."{seq_name}";'
            )
            dst_current[seq_name] = val

        await load_sequences(dst_pool, non_pk_vals, schema, dst_logger)

        for name, target_val in non_pk_vals.items():
            dst_val = dst_current.get(name, 0)
            synced = target_val >= dst_val
            method = "source_value_with_stride" if stride else "source_value"
            detail: dict[str, Any] = {
                "name": name,
                "source_value": original_vals[name],
                "destination_value": target_val if synced else dst_val,
                "synced": synced,
                "method": method,
            }
            if not synced:
                detail["skipped_reason"] = "destination value is ahead of source"
            non_pk_details.append(detail)
    elif not pk_seqs:
        dst_logger.info("No sequences to sync.")

    return {
        "schema_name": schema,
        "stride": stride,
        "pk_sequences": pk_details,
        "non_pk_sequences": non_pk_details,
    }


@run_with_configs
async def sync_sequences(
    config_future: Awaitable[DbupgradeConfig],
    stride: int | None = Option(
        None,
        "--stride",
        help=(
            "Pad non-PK sequences by this amount when syncing: "
            "loads source_value + stride. "
            "Recommended default: --stride 1000."
        ),
    ),
) -> dict[str, Any] | None:
    """
    Sync all sequences to the destination database.

    For sequences that back primary key columns, the value is set from
    max(pk_column) on the destination table — this is always the safest baseline.

    For all other sequences, the current value is read from the source and applied
    to the destination, but only if the source value is >= the current destination
    value. This prevents regressing sequences if run after cutover.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.pglogical_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_pool, dst_pool = pools
    try:
        src_logger = get_logger(conf.db, conf.dc, "sync.src")
        dst_logger = get_logger(conf.db, conf.dc, "sync.dst")
        return await _sync_sequences(
            conf.sequences,
            conf.schema_name,
            src_pool,
            dst_pool,
            src_logger,
            dst_logger,
            stride=stride,
        )
    finally:
        await gather(*[p.close() for p in pools])


@run_with_configs
async def sync_tables(
    config_future: Awaitable[DbupgradeConfig],
    table: list[str] = Option([], help="Specific tables to sync"),
) -> dict[str, Any] | None:
    """
    Dump tables without primary keys from the source and pipe them directly
    into the destination database. No intermediate files are used -- data is
    streamed via pg_dump | psql.

    A table will only be loaded into the destination if it currently contains
    no rows.

    You may also provide specific PK-less tables to sync with the --table option.
    Need to run like --table table1 --table table2 ...
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "sync")

    if table:
        tables = table
        discovery_mode = "explicit"
    else:
        discovery_mode = "auto"
        async with create_pool(conf.src.pglogical_uri, min_size=1) as src_pool:
            _, tables, _ = await analyze_table_pkeys(src_pool, conf.schema_name, logger)

        if conf.tables:
            tables = [t for t in tables if t in conf.tables]

    table_details = await dump_and_load_tables_with_details(conf, tables, logger)

    return {
        "schema_name": conf.schema_name,
        "discovery_mode": discovery_mode,
        "tables": table_details,
    }


@run_with_configs(skip_src=True)
async def analyze(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Run ANALYZE in the destination database. This should be run after data is
    completely replicated and before applications are allowed to use the new db.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "sync.dst")
    async with create_pool(
        conf.dst.root_uri,
        min_size=1,
        server_settings={
            "statement_timeout": "0",
        },
    ) as dst_pool:
        await run_analyze(dst_pool, logger)


@run_with_configs
async def validate_data(
    config_future: Awaitable[DbupgradeConfig],
) -> dict[str, Any] | None:
    """
    Compares data in the source and target databases. Both a random sample and a
    sample of the latest rows will be compared for each table. Does not validate
    the entire data set.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.pglogical_uri, min_size=1),
        create_pool(conf.dst.owner_uri, min_size=1),
    )
    src_pool, dst_pool = pools

    validations: list[dict] = []

    async def _run_validation(coro, strategy: str) -> None:
        try:
            await coro
            validations.append({"name": strategy, "strategy": strategy, "passed": True})
        except Exception as e:
            validations.append(
                {
                    "name": strategy,
                    "strategy": strategy,
                    "passed": False,
                    "mismatch_detail": str(e),
                }
            )

    try:
        logger = get_logger(conf.db, conf.dc, "sync")
        await gather(
            _run_validation(
                compare_100_random_rows(
                    src_pool, dst_pool, conf.tables, conf.schema_name, logger
                ),
                "random_100",
            ),
            _run_validation(
                compare_latest_100_rows(
                    src_pool, dst_pool, conf.tables, conf.schema_name, logger
                ),
                "latest_100",
            ),
            _run_validation(
                compare_tables_without_pkeys(
                    src_pool, dst_pool, conf.tables, conf.schema_name, logger
                ),
                "no_pkey_presence",
            ),
        )
    finally:
        await gather(*[p.close() for p in pools])

    return {
        "schema_name": conf.schema_name,
        "tables": validations,
    }


async def _dump_and_load_all_tables(
    conf: DbupgradeConfig, src_pool: Pool, src_logger: Logger, dst_logger: Logger
) -> None:
    _, tables, _ = await analyze_table_pkeys(src_pool, conf.schema_name, src_logger)
    if conf.tables:
        tables = [t for t in tables if t in conf.tables]
    await dump_and_load_tables(conf, tables, dst_logger)


@run_with_configs
async def sync(
    config_future: Awaitable[DbupgradeConfig], no_schema: bool = False
) -> None:
    """
    Sync and validate all data that is not replicated with pglogical. This includes all
    tables without primary keys and all sequences. Also loads any previously omitted
    NOT VALID constraints into the destination db and runs ANALYZE in the destination.

    This command is equivalent to running the following commands in order:
    sync-sequences, sync-tables, validate-data, load-constraints, analyze.
    Though here they may run concurrently when possible.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.pglogical_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
        create_pool(conf.dst.owner_uri, min_size=1),
        create_pool(
            conf.dst.root_uri,
            min_size=1,
            server_settings={
                "statement_timeout": "0",
            },
        ),
    )
    src_pool, dst_root_pool, dst_owner_pool, dst_root_no_timeout_pool = pools

    try:
        src_logger = get_logger(conf.db, conf.dc, "sync.src")
        dst_logger = get_logger(conf.db, conf.dc, "sync.dst")
        validation_logger = get_logger(conf.db, conf.dc, "sync")

        await gather(
            _sync_sequences(
                conf.sequences,
                conf.schema_name,
                src_pool,
                dst_root_pool,
                src_logger,
                dst_logger,
            ),
            _dump_and_load_all_tables(conf, src_pool, src_logger, dst_logger),
        )

        # Creating indexes should run before validations and ANALYZE, but after all the data exists
        # in the destination database.

        # Do not load NOT VALID constraints or create INDEXes for exodus-style migrations
        if not no_schema:
            await gather(
                apply_target_constraints(conf, dst_logger),
                create_target_indexes(conf, dst_logger, during_sync=True),
            )

        await gather(
            compare_100_random_rows(
                src_pool,
                dst_owner_pool,
                conf.tables,
                conf.schema_name,
                validation_logger,
            ),
            compare_latest_100_rows(
                src_pool,
                dst_owner_pool,
                conf.tables,
                conf.schema_name,
                validation_logger,
            ),
            compare_tables_without_pkeys(
                src_pool,
                dst_owner_pool,
                conf.tables,
                conf.schema_name,
                validation_logger,
            ),
            run_analyze(dst_root_no_timeout_pool, dst_logger),
        )
    finally:
        await gather(*[p.close() for p in pools])


async def _print_diff_sequences_table(results: list[dict[str, Any]]) -> None:
    first_db = True
    for r in sorted(results, key=lambda d: d.get("db", "")):
        if not first_db:
            echo("")
        first_db = False
        echo(style(r.get("db", ""), "green"))
        table: list[list[Any]] = [
            [
                style("sequence", "yellow"),
                style("SRC", "yellow"),
                style("DST", "yellow"),
            ]
        ]
        for row in sorted(r.get("sequences", []), key=lambda x: x["name"]):
            src_v = row.get("source_value")
            dst_v = row.get("destination_value")
            dst_ok = row.get("destination_ok", False)
            src_cell = str(src_v) if src_v is not None else "—"
            if dst_v is None:
                dst_cell = style("—", "yellow")
            else:
                dst_cell = style(str(dst_v), "green" if dst_ok else "red")
            table.append([row["name"], src_cell, dst_cell])
        echo(tabulate(table, headers="firstrow"))


@run_with_configs(results_callback=_print_diff_sequences_table)
async def diff_sequences(
    config_future: Awaitable[DbupgradeConfig],
) -> dict[str, Any]:
    """
    Compare source and destination sequence last_value for each targeted sequence.

    Destination values are highlighted green when they are greater than or equal to
    source, and red otherwise.

    Requires both src and dst to be not null in the config file.

    If the db name is not given, runs for every database in the datacenter (same
    pattern as ``diff-schemas``).
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "sync.diff_sequences")
    pools = await gather(
        create_pool(conf.src.pglogical_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_pool, dst_pool = pools
    try:
        src_map, dst_map = await gather(
            dump_sequences(src_pool, conf.sequences, conf.schema_name, logger),
            dump_sequences(dst_pool, conf.sequences, conf.schema_name, logger),
        )
    finally:
        await gather(*[p.close() for p in pools])

    names = sorted(set(src_map.keys()) | set(dst_map.keys()))
    sequences_out: list[dict[str, Any]] = []
    for name in names:
        sv = _sequence_value_as_int(src_map.get(name))
        dv = _sequence_value_as_int(dst_map.get(name))
        if sv is not None and dv is not None:
            dst_ok = dv >= sv
        elif sv is None and dv is not None:
            dst_ok = True
        elif sv is not None and dv is None:
            dst_ok = False
        else:
            dst_ok = True
        sequences_out.append(
            {
                "name": name,
                "source_value": sv,
                "destination_value": dv,
                "destination_ok": dst_ok,
            }
        )

    overall = "match" if all(s["destination_ok"] for s in sequences_out) else "mismatch"
    return {
        "db": conf.db,
        "schema_name": conf.schema_name,
        "sequences": sequences_out,
        "result": overall,
    }


COMMANDS = [
    sync_sequences,
    sync_tables,
    analyze,
    validate_data,
    sync,
    diff_sequences,
]
