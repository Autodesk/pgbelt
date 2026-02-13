from asyncio import gather
from collections.abc import Awaitable
from logging import Logger

from asyncpg import Pool
from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import dump_source_schema
from pgbelt.util.dump import remove_dst_indexes
from pgbelt.util.dump import remove_dst_not_valid_constraints
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import analyze_table_pkeys
from pgbelt.util.pglogical import teardown_subscription


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _truncate_dst_tables(
    conf: DbupgradeConfig, dst_pool: Pool, logger: Logger
) -> None:
    if conf.tables:
        tables = conf.tables
    else:
        pkey_tables, no_pkey_tables, _ = await analyze_table_pkeys(
            dst_pool, conf.schema_name, logger
        )
        tables = list(set(pkey_tables + no_pkey_tables))

    if not tables:
        logger.info("No destination tables found to truncate. Skipping.")
        return

    logger.info(f"Truncating destination tables in schema {conf.schema_name}: {tables}")
    schema = _quote_ident(conf.schema_name)
    async with dst_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL lock_timeout = '2s';")
            for table in tables:
                await conn.execute(
                    f"TRUNCATE TABLE {schema}.{_quote_ident(table)} CASCADE;"
                )
    logger.debug("Finished truncating destination tables.")


@run_with_configs
async def reset(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Reset an in-progress migration before cutover so replication can be started
    again from the beginning.

    This command:
    1) Stops forward replication
    2) Ensures reverse replication is stopped
    3) Truncates destination tables (all tables in schema, or only config.tables)
    4) Removes indexes from the destination
    5) Removes NOT VALID constraints from the destination

    Note: sequence values are intentionally left unchanged. They only need to be
    synchronized after cutover by running sync-sequences.
    """
    conf = await config_future
    src_logger = get_logger(conf.db, conf.dc, "reset.src")
    dst_logger = get_logger(conf.db, conf.dc, "reset.dst")

    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_pool, dst_pool = pools
    try:
        await gather(
            teardown_subscription(dst_pool, "pg1_pg2", dst_logger),
            teardown_subscription(src_pool, "pg2_pg1", src_logger),
        )
        await _truncate_dst_tables(conf, dst_pool, dst_logger)

        # Ensure schema artifacts are present/refreshed for remove_* routines.
        await dump_source_schema(conf, src_logger)
        await remove_dst_indexes(conf, dst_logger)
        await remove_dst_not_valid_constraints(conf, dst_logger)
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [reset]
