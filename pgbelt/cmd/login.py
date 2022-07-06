import asyncio
from logging import Logger
from typing import Awaitable

from asyncpg import create_pool
from asyncpg import Pool

from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User
from pgbelt.util import get_logger
from pgbelt.util.postgres import disable_login_users
from pgbelt.util.postgres import enable_login_users
from pgbelt.util.postgres import get_login_users


# TODO this should be configurable, add a PgbeltConfig or something
NO_DISABLE = [
    "pglogical",
    "postgres",
    "rdsadmin",
    "vividcortexsu",
    "vividcortex",
    "fivetran",
    "datadog",
    "rdsrepladmin",
    "monitoring",
]


async def _populate_logins(dbconf: DbConfig, pool: Pool, logger: Logger) -> None:
    all_logins = await get_login_users(pool, logger)
    exclude = [
        dbconf.root_user.name,
        dbconf.owner_user.name,
        dbconf.pglogical_user.name,
    ]
    dbconf.other_users = [User(name=n) for n in all_logins if n not in exclude]


@run_with_configs(skip_dst=True)
async def revoke_logins(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Discovers all users in the db who can log in, saves them in the config file,
    then revokes their permission to log in. Use this command to ensure that all
    writes to the source database have been stopped before syncing sequence values
    and tables without primary keys.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "login.src")

    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        save_task = None
        if conf.src.other_users is None:
            await _populate_logins(conf.src, pool, logger)
            save_task = asyncio.create_task(conf.save())

        to_disable = [conf.src.owner_user.name]

        if conf.src.other_users is not None:
            to_disable += [
                u.name for u in conf.src.other_users if u.name not in NO_DISABLE
            ]

        try:
            await disable_login_users(pool, to_disable, logger)
        finally:
            if save_task is not None:
                await save_task


@run_with_configs(skip_dst=True)
async def restore_logins(config_future: Awaitable[DbupgradeConfig]) -> None:
    """
    Grant permission to log in for any user present in the config file. The user
    must already have a password. This will not generate or modify existing
    passwords for users.

    Intended to be used after revoke-logins in case a rollback is required.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "login.src")
    to_enable = [conf.src.owner_user.name]

    if conf.src.other_users is not None:
        to_enable += [u.name for u in conf.src.other_users if u.name not in NO_DISABLE]

    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        await enable_login_users(pool, to_enable, logger)


COMMANDS = [
    revoke_logins,
    restore_logins,
]
