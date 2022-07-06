from typing import Awaitable

from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.dump import apply_target_constraints
from pgbelt.util.dump import apply_target_schema
from pgbelt.util.dump import dump_dst_not_valid_constraints
from pgbelt.util.dump import dump_source_schema
from pgbelt.util.dump import remove_dst_not_valid_constraints
from pgbelt.util.logs import get_logger


@run_with_configs
async def dump_schema(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Dumps and sanitizes the schema from the source database, then saves it to
    a file. Three files will be generated. One contains the entire sanitized
    schema, one contains the schema with all NOT VALID constraints removed, and
    another contains only the NOT VALID constraints that were removed. These
    files will be saved in the schemas directory.
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
async def dump_constraints(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Dumps the NOT VALID constraints from the target database onto disk, in
    the schemas directory.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "schema.dst")
    await dump_dst_not_valid_constraints(conf, logger)


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


COMMANDS = [
    dump_schema,
    load_schema,
    load_constraints,
    dump_constraints,
    remove_constraints,
]
