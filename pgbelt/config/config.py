import asyncio
from os.path import join
from typing import AsyncGenerator
from typing import Awaitable
from typing import Optional

from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.remote import resolve_remote_config
from pgbelt.util import get_logger
from pgbelt.util.asyncfuncs import isdir
from pgbelt.util.asyncfuncs import isfile
from pgbelt.util.asyncfuncs import listdir


def get_config(
    db: str, dc: str, skip_src: bool = False, skip_dst: bool = False
) -> Optional[DbupgradeConfig]:
    """
    Get a configuration for one database pair synchronously.
    """
    config = asyncio.run(get_config_async(db, dc, skip_src, skip_dst))

    if config is None:
        exit(1)

    return config


async def get_config_async(
    db: str, dc: str, skip_src: bool = False, skip_dst: bool = False
) -> Optional[DbupgradeConfig]:
    """
    Get configuration for one database pair asynchronously. Always prefers
    locally cached configuration but attempts to resolve any uncached configuration
    if it is required. Locally cached config never expires, so it may become stale.
    """
    logger = get_logger(db, dc, "config")
    logger.info("Getting configuration...")
    config = await DbupgradeConfig.load(db, dc)

    if config is None or (config.src is None and config.dst is None):
        logger.info("Resolving remote configuration...")
        config = await resolve_remote_config(db, dc, skip_src, skip_dst)
    elif config.src is None and not skip_src:
        logger.info("Cached config did not include source info, resolving...")
        src_config = await resolve_remote_config(db, dc, skip_dst=True)
        if src_config is None or src_config.src is None:
            logger.critical("Could not resolve missing source info!")
            return None
        config.src = src_config.src
    elif config.dst is None and not skip_dst:
        logger.info("Cached config did not include target info, resolving...")
        dst_config = await resolve_remote_config(db, dc, skip_src=True)
        if dst_config is None or dst_config.dst is None:
            logger.critical("Could not resolve missing target info!")
            return None
        config.dst = dst_config.dst
    else:
        return config

    if config is None or (config.src is None and config.dst is None):
        logger.critical("No configuration could be retrieved")
    else:
        await config.save()

    return config


async def find_available_configs(confdir: str, dc: str) -> set[str]:
    """
    Search for all the database configs in a datacenter directory within a config directory.
    """
    result = set()
    dc_dir = join(confdir, dc)
    if not await (isdir(dc_dir)):
        return set()
    for db in await listdir(dc_dir):
        if await isdir(join(dc_dir, db)) and await isfile(
            join(dc_dir, db, "config.json")
        ):
            result.add(db)
    return result


async def get_all_configs_async(
    dc: str, skip_src: bool = False, skip_dst: bool = False
) -> AsyncGenerator[Awaitable[Optional[DbupgradeConfig]], None]:
    """
    A generator that produces Awaitables that resolve to DbupgradeConfigs or None

    Will produce all possible configs in the given dc whether they are remote
    or already resolved and cached.
    """
    logger = get_logger("all", dc, "config")
    logger.info("Getting all available configurations...")
    remote, local = await asyncio.gather(
        find_available_configs("remote-configs", dc),
        find_available_configs("configs", dc),
    )

    for conf in asyncio.as_completed(
        [
            get_config_async(db, dc, skip_src=skip_src, skip_dst=skip_dst)
            for db in remote | local
        ]
    ):
        yield conf
