from collections.abc import Awaitable
from asyncpg import create_pool

from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import apply_target_constraints
from pgbelt.util.dump import apply_target_schema
from pgbelt.util.dump import create_target_indexes
from pgbelt.util.dump import dump_source_schema
from pgbelt.util.dump import remove_dst_not_valid_constraints
from pgbelt.util.dump import remove_dst_indexes
from pgbelt.util.logs import get_logger
from pgbelt.util.postgres import run_analyze


@run_with_configs
async def dump_schema(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Dumps and sanitizes the schema from the source database, then saves it to
    a file. Four files will be generated:
    1. The entire sanitized schema
    2. The schema with all NOT VALID constraints and CREATE INDEX statements removed,
    3. A file that contains only the CREATE INDEX statements
    4. A file that contains only the NOT VALID constraints
    These files will be saved in the schemas directory.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.src")
    await dump_source_schema(conf, logger)


@run_with_configs(skip_src=True)
async def load_schema(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Loads the sanitized schema from the file schemas/dc/db/no_invalid_constraints.sql
    into the destination as the owner user.

    Invalid constraints are omitted because the source database may contain data
    that was created before the constraint was added. Loading the constraints into
    the destination before the data will cause replication to fail.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await apply_target_schema(conf, logger)


@run_with_configs(skip_src=True)
async def load_constraints(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Loads the NOT VALID constraints from the file schemas/dc/db/invalid_constraints.sql
    into the destination as the owner user. This must only be done after all data is
    synchronized from the source to the destination database.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await apply_target_constraints(conf, logger)


@run_with_configs(skip_src=True)
async def remove_constraints(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Removes NOT VALID constraints from the target database. This must be done
    before setting up replication, and should only be used if the schema in the
    target database was loaded outside of pgbelt.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await remove_dst_not_valid_constraints(conf, logger)


@run_with_configs(skip_src=True)
async def remove_indexes(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Removes indexes from the target database. This must be done
    before setting up replication, and should only be used if the schema in the
    target database was loaded outside of pgbelt.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await remove_dst_indexes(conf, logger)


@run_with_configs(skip_src=True)
async def create_indexes(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Creates indexes from the file schemas/dc/db/indexes.sql into the destination
    as the owner user. This must only be done after most data is synchronized
    (at minimum after the initializing phase) from the source to the destination
    database.

    After creating indexes, the destination database should be analyzed to ensure
    the query planner has the most up-to-date statistics for the indexes.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await create_target_indexes(conf, logger, during_sync=False)

    # Run ANALYZE after creating indexes (without statement timeout)
    async with create_pool(
        conf.dst.root_uri,
        min_size=1,
        server_settings={
            "statement_timeout": "0",
        },
    ) as dst_pool:
        await run_analyze(dst_pool, logger)


COMMANDS = [
    dump_schema,
    load_schema,
    load_constraints,
    remove_constraints,
    remove_indexes,
    create_indexes,
]
