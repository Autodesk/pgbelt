from asyncio import gather
from collections.abc import Awaitable
from logging import Logger

from asyncpg import create_pool
from asyncpg import Pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import apply_target_constraints
from pgbelt.util.dump import create_target_indexes
from pgbelt.util.dump import dump_source_tables
from pgbelt.util.dump import load_dumped_tables
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.postgres import compare_100_rows
from pgbelt.util.postgres import compare_latest_100_rows
from pgbelt.util.postgres import dump_sequences
from pgbelt.util.postgres import load_sequences
from pgbelt.util.postgres import run_analyze
from typer import Option


async def _sync_sequences(
    targeted_sequences: list[str],
    schema: str,
    src_pool: Pool,
    dst_pool: Pool,
    src_logger: Logger,
    dst_logger: Logger,
) -> None:

    seq_vals = await dump_sequences(src_pool, targeted_sequences, schema, src_logger)
    await load_sequences(dst_pool, seq_vals, schema, dst_logger)


@run_with_configs
async def sync_sequences(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Retrieve the current value of all sequences in the source database and update
    the sequences in the target to match.
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
        await _sync_sequences(
            conf.sequences, conf.schema_name, src_pool, dst_pool, src_logger, dst_logger
        )
    finally:
        await gather(*[p.close() for p in pools])


@run_with_configs(skip_dst=True)
async def dump_tables(
    config_future: Awaitable[DbupgradeConfig],
    tables: list[str] = Option([], help="Specific tables to dump"),
) -> None:
    """
    Dump all tables without primary keys from the source database and save
    them to files locally.

    You may also provide a list of tables to dump with the
    --tables option and only these tables will be dumped.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "sync.src")

    if tables:
        tables = tables.split(",")
    else:
        async with create_pool(conf.src.pglogical_uri, min_size=1) as src_pool:
            _, tables, _ = await analyze_table_pkeys(src_pool, conf.schema_name, logger)

        if conf.tables:
            tables = [t for t in tables if t in conf.tables]

    await dump_source_tables(conf, tables, logger)


@run_with_configs(skip_src=True)
async def load_tables(
    config_future: Awaitable[DbupgradeConfig],
    tables: list[str] = Option([], help="Specific tables to load"),
):
    """
    Load all locally saved table data files into the destination db. A table will
    only be loaded into the destination if it currently contains no rows.

    You may also provide a list of tables to load with the
    --tables option and only these files will be loaded.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "sync.dst")

    if tables:
        tables = tables.split(",")
    else:
        if conf.tables:
            tables = [t for t in tables if t in conf.tables]
        else:
            tables = []

    await load_dumped_tables(conf, tables, logger)


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
async def validate_data(config_future: Awaitable[DbupgradeConfig]) -> None:
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

    try:
        logger = get_logger(conf.db, conf.dc, "sync")
        await gather(
            compare_100_rows(src_pool, dst_pool, conf.tables, conf.schema_name, logger),
            compare_latest_100_rows(
                src_pool, dst_pool, conf.tables, conf.schema_name, logger
            ),
        )
    finally:
        await gather(*[p.close() for p in pools])


async def _dump_and_load_all_tables(
    conf: DbupgradeConfig, src_pool: Pool, src_logger: Logger, dst_logger: Logger
) -> None:
    _, tables, _ = await analyze_table_pkeys(src_pool, conf.schema_name, src_logger)
    if conf.tables:
        tables = [t for t in tables if t in conf.tables]
    await dump_source_tables(conf, tables, src_logger)
    await load_dumped_tables(conf, tables, dst_logger)


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
            compare_100_rows(
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
            run_analyze(dst_root_no_timeout_pool, dst_logger),
        )
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [
    sync_sequences,
    dump_tables,
    load_tables,
    analyze,
    validate_data,
    sync,
]
