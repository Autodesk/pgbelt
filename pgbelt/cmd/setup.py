from asyncio import create_task
from asyncio import gather
from collections.abc import Awaitable
from logging import Logger

from asyncpg import create_pool
from asyncpg import Pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import apply_target_schema
from pgbelt.util.dump import dump_source_schema
from pgbelt.util.logs import get_logger
from pgbelt.util.pglogical import configure_node
from pgbelt.util.pglogical import configure_pgl
from pgbelt.util.pglogical import configure_replication_set
from pgbelt.util.pglogical import configure_subscription
from pgbelt.util.pglogical import grant_pgl
from pgbelt.util.postgres import analyze_table_pkeys
from typer import Option


async def _dump_and_load_schema(
    conf: DbupgradeConfig, src_logger: Logger, dst_logger: Logger
) -> None:
    await dump_source_schema(conf, src_logger)
    await apply_target_schema(conf, dst_logger)


async def _setup_src_node(
    conf: DbupgradeConfig, src_root_pool: Pool, src_logger: Logger
) -> None:
    """
    Configure the pglogical node and replication set on the Source database.
    """

    await configure_node(src_root_pool, "pg1", conf.src.pglogical_dsn, src_logger)
    async with create_pool(conf.src.pglogical_uri, min_size=1) as src_pglogical_pool:
        pkey_tables, _, _ = await analyze_table_pkeys(
            src_pglogical_pool, conf.schema_name, src_logger
        )

    pglogical_tables = pkey_tables
    if conf.tables:
        pglogical_tables = [
            t
            for t in pkey_tables
            if t
            in list(
                map(str.lower, conf.tables)
            )  # Postgres returns table names in lowercase (in analyze_table_pkeys)
        ]

    # Intentionally throw an error if no tables are found, so that the user can correct their config.
    # When reported by a certain user, errors showed when running the status command, but it was ignored,
    # then the user ran setup and since that DIDN'T throw an error, they assumed everything was fine.

    if not pglogical_tables:
        raise ValueError(
            f"No tables were targeted to replicate. Please check your config's schema and tables. DB: {conf.db} DC: {conf.dc}, SCHEMA: {conf.schema_name} TABLES: {conf.tables}.\nIf TABLES is [], all tables in the schema should be replicated, but pgbelt still found no tables.\nCheck the schema name or reach out to the pgbelt team for help."
        )

    await configure_replication_set(
        src_root_pool, pglogical_tables, conf.schema_name, src_logger
    )


@run_with_configs
async def setup(
    config_future: Awaitable[DbupgradeConfig],
    schema: bool = Option(True, help="Copy the schema?"),
) -> None:
    """
    Configures pglogical to replicate all compatible tables from the source
    to the destination db. This includes copying the database schema from the
    source into the destination.

    If you want to set up the schema in the destination db manually you can use
    the --no-schema option to stop this from happening.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.src.owner_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
        create_pool(conf.dst.owner_uri, min_size=1),
    )

    src_root_pool, src_owner_pool, dst_root_pool, dst_owner_pool = pools
    try:
        src_logger = get_logger(conf.db, conf.dc, "setup.src")
        dst_logger = get_logger(conf.db, conf.dc, "setup.dst")

        # Configure Source for pglogical (before we can configure the plugin)
        await configure_pgl(
            src_root_pool,
            conf.src.pglogical_user.pw,
            src_logger,
            conf.src.owner_user.name,
        )
        await grant_pgl(src_owner_pool, conf.tables, conf.schema_name, src_logger)

        # Load schema into destination
        schema_load_task = None
        if schema:
            schema_load_task = create_task(
                _dump_and_load_schema(conf, src_logger, dst_logger)
            )

        # Configure Pglogical plugin on Source
        src_node_task = create_task(_setup_src_node(conf, src_root_pool, src_logger))

        # We need to wait for the schema to exist in the target before setting up pglogical there
        if schema_load_task is not None:
            await schema_load_task

        # Configure Destination for pglogical (before we can configure the plugin)
        await configure_pgl(
            dst_root_pool,
            conf.dst.pglogical_user.pw,
            dst_logger,
            conf.dst.owner_user.name,
        )
        await grant_pgl(dst_owner_pool, conf.tables, conf.schema_name, dst_logger)

        # Also configure the node on the destination... of itself. #TODO: This is a bit weird, confirm if this is necessary.
        await configure_node(dst_root_pool, "pg2", conf.dst.pglogical_dsn, dst_logger)

        # The source node must be set up before we create a subscription
        await src_node_task
        await configure_subscription(
            dst_root_pool, "pg1_pg2", conf.src.pglogical_dsn, dst_logger
        )
    finally:
        await gather(*[p.close() for p in pools])


@run_with_configs
async def setup_back_replication(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Configures pglogical to replicate all compatible tables from the destination
    to the source db. Can only complete successfully after the initial load phase
    is completed for replication from the source to target.

    Back replication ensures that dataloss does not occur if a rollback is required
    after applications are allowed to begin writing data into the destination db.
    """
    conf = await config_future
    pools = await gather(
        create_pool(conf.src.root_uri, min_size=1),
        create_pool(conf.src.pglogical_uri, min_size=1),
        create_pool(conf.dst.root_uri, min_size=1),
    )
    src_root_pool, src_pglogical_pool, dst_root_pool = pools

    try:
        src_logger = get_logger(conf.db, conf.dc, "setup.src")
        pkeys, _, _ = await analyze_table_pkeys(
            src_pglogical_pool, conf.schema_name, src_logger
        )
        dst_logger = get_logger(conf.db, conf.dc, "setup.src")

        pglogical_tables = pkeys
        if conf.tables:
            pglogical_tables = [
                t
                for t in pkeys
                if t
                in list(
                    map(str.lower, conf.tables)
                )  # Postgres returns table names in lowercase (in analyze_table_pkeys)
            ]

        await configure_replication_set(
            dst_root_pool, pglogical_tables, conf.schema_name, dst_logger
        )
        await configure_subscription(
            src_root_pool, "pg2_pg1", conf.dst.pglogical_dsn, src_logger
        )
    finally:
        await gather(*[p.close() for p in pools])


COMMANDS = [
    setup,
    setup_back_replication,
]
