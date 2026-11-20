from asyncio import gather
from asyncio import sleep
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dblink import teardown_dblink
from pgbelt.util.logs import get_logger
from pgbelt.util.pglogical import cleanup_all_pglogical
from pgbelt.util.pglogical import teardown_pgl
from pgbelt.util.pglogical import teardown_replication_set
from pgbelt.util.pglogical import teardown_shared_pglogical
from pgbelt.util.pglogical import teardown_subscription
from typer import Option


@run_with_configs(skip_dst=True)
async def teardown_back_replication(config_future: Awaitable[DbupgradeConfig]):
    """
    Stops pglogical replication from the destination database to the source.
    You should only do this once you are certain a rollback will not be required.
    """
    conf = await config_future
    async with create_pool(conf.src.root_uri, min_size=1) as src_pool:
        logger = get_logger(conf.db, conf.dc, "teardown.src")
        await teardown_subscription(src_pool, conf.back_subscription, logger)


@run_with_configs(skip_src=True)
async def teardown_forward_replication(config_future: Awaitable[DbupgradeConfig]):
    """
    Stops pglogical replication from the source database to the destination.
    This should be done during your migration downtime before writes are allowed
    to the destination.
    """
    conf = await config_future
    async with create_pool(conf.dst.root_uri, min_size=1) as dst_pool:
        logger = get_logger(conf.db, conf.dc, "teardown.dst")
        await teardown_subscription(dst_pool, conf.forward_subscription, logger)


@run_with_configs
async def teardown(
    config_future: Awaitable[DbupgradeConfig],
    full: bool = Option(False, help="Remove pglogical and dblink extensions"),
):
    """
    Removes all pglogical configuration from both databases. If any replication is
    configured this will stop it. It will also drop the pglogical user.

    When other pgbelt migrations still use a database, for example other schemas
    from the same schema_names config, only this migration's replication is removed.
    The shared node, pglogical user and extensions stay until the last one is torn down.

    If run with --full the pglogical and dblink extensions will be dropped.

    WARNING: running with --full may cause the database to lock up. You should be
    prepared to reboot the database if you do this.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_root_pool, dst_root_pool = pools

    try:
        src_logger = get_logger(conf.db, conf.dc, "teardown.src")
        dst_logger = get_logger(conf.db, conf.dc, "teardown.dst")

        await gather(
            teardown_subscription(src_root_pool, conf.back_subscription, src_logger),
            teardown_subscription(dst_root_pool, conf.forward_subscription, dst_logger),
        )

        await gather(
            teardown_replication_set(src_root_pool, src_logger, conf.replication_set),
            teardown_replication_set(dst_root_pool, dst_logger, conf.replication_set),
        )
        await sleep(15)

        src_unused, dst_unused = await gather(
            teardown_shared_pglogical(
                src_root_pool, "pg1", conf.tables, conf.schema_name, src_logger
            ),
            teardown_shared_pglogical(
                dst_root_pool, "pg2", conf.tables, conf.schema_name, dst_logger
            ),
        )

        if full:
            await sleep(15)

            unused = [
                (pool, logger)
                for pool, logger, is_unused in [
                    (src_root_pool, src_logger, src_unused),
                    (dst_root_pool, dst_logger, dst_unused),
                ]
                if is_unused
            ]
            await gather(*[teardown_dblink(pool, logger) for pool, logger in unused])
            await gather(*[teardown_pgl(pool, logger) for pool, logger in unused])
    finally:
        await gather(*[p.close() for p in pools])


@run_with_configs(skip_dst=True)
async def cleanup_previous_config(
    config_future: Awaitable[DbupgradeConfig],
    full: bool = Option(False, help="Remove pglogical extension"),
):
    """
    Removes any pglogical configuration left over from a previous migration on the
    source database. This is useful when the source database was previously a
    destination in an earlier migration and still has pglogical artifacts.

    Tries to clean up all pglogical subscriptions, replication sets, nodes, and the
    pglogical role regardless of what role the database previously played.

    If run with --full the pglogical extension will be dropped.

    WARNING: running with --full may cause the database to lock up. You should be
    prepared to reboot the database if you do this.
    """
    conf = await config_future
    async with create_pool(conf.src.root_uri, min_size=1) as src_pool:
        logger = get_logger(conf.db, conf.dc, "cleanup.src")
        await cleanup_all_pglogical(src_pool, conf.tables, conf.schema_name, logger)

        if full:
            await sleep(15)
            await teardown_pgl(src_pool, logger)


COMMANDS = [
    teardown_back_replication,
    teardown_forward_replication,
    teardown,
    cleanup_previous_config,
]
