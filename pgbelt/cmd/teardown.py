from asyncio import gather
from asyncio import sleep
from typing import Awaitable

from asyncpg import create_pool
from typer import Option

from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pgbelt.util.pglogical import revoke_pgl
from pgbelt.util.pglogical import teardown_node
from pgbelt.util.pglogical import teardown_pgl
from pgbelt.util.pglogical import teardown_replication_set
from pgbelt.util.pglogical import teardown_subscription


@run_with_configs(skip_dst=True)
async def teardown_back_replication(config_future: Awaitable[DbupgradeConfig]):
    """
    Stops pglogical replication from the destination database to the source.
    You should only do this once you are certain a rollback will not be required.
    """
    conf = await config_future
    async with create_pool(conf.src.root_uri, min_size=1) as src_pool:
        logger = get_logger(conf.db, conf.dc, "teardown.src")
        await teardown_subscription(src_pool, "pg2_pg1", logger)


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
        await teardown_subscription(dst_pool, "pg1_pg2", logger)


@run_with_configs
async def teardown(
    config_future: Awaitable[DbupgradeConfig],
    full: bool = Option(False, help="Remove pglogical user and extension"),
):
    """
    Removes all pglogical configuration from both databases. If any replication is
    configured this will stop it.

    If run with --full the pglogical users and extension will be dropped.

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
            teardown_subscription(src_root_pool, "pg2_pg1", src_logger),
            teardown_subscription(dst_root_pool, "pg1_pg2", dst_logger),
        )

        await gather(
            teardown_replication_set(src_root_pool, src_logger),
            teardown_replication_set(dst_root_pool, dst_logger),
        )
        await sleep(15)

        await gather(
            teardown_node(src_root_pool, "pg1", src_logger),
            teardown_node(dst_root_pool, "pg2", dst_logger),
        )

        if full:
            await sleep(15)
            async with create_pool(conf.src.owner_uri, min_size=1) as src_owner_pool:
                async with create_pool(
                    conf.dst.owner_uri, min_size=1
                ) as dst_owner_pool:
                    await gather(
                        revoke_pgl(src_owner_pool, conf.tables, src_logger),
                        revoke_pgl(dst_owner_pool, conf.tables, dst_logger),
                    )

            await gather(
                teardown_pgl(src_root_pool, src_logger),
                teardown_pgl(dst_root_pool, dst_logger),
            )
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [teardown_back_replication, teardown_forward_replication, teardown]
