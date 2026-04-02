import asyncio
import re
from collections.abc import Awaitable
from logging import Logger

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
from typer import Option


# TODO this should be configurable, add a PgbeltConfig or something
NO_DISABLE = [
    "pglogical",
    "postgres",
    "rdsadmin",
    "rdsrepladmin",
    "datadog",
    "monitoring",
]


def _like_to_regex(pattern: str) -> re.Pattern:
    """Convert a SQL LIKE pattern (% and _) to a compiled regex."""
    parts = []
    for ch in pattern:
        if ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    return re.compile(f"^{''.join(parts)}$", re.IGNORECASE)


def _is_excluded(
    username: str,
    exclude_users: list[str],
    exclude_patterns: list[re.Pattern],
) -> bool:
    if username in exclude_users:
        return True
    return any(p.match(username) for p in exclude_patterns)


async def _populate_logins(dbconf: DbConfig, pool: Pool, logger: Logger) -> None:
    all_logins = await get_login_users(pool, logger)
    exclude = [
        dbconf.root_user.name,
        dbconf.owner_user.name,
        dbconf.pglogical_user.name,
    ]
    dbconf.other_users = [User(name=n) for n in all_logins if n not in exclude]


@run_with_configs(skip_dst=True)
async def revoke_logins(
    config_future: Awaitable[DbupgradeConfig],
    exclude_users: list[str] = Option(
        [],
        "--exclude-user",
        "-e",
        help="Additional usernames to exclude from revocation (can be repeated).",
    ),
    exclude_patterns: list[str] = Option(
        [],
        "--exclude-pattern",
        "-p",
        help="SQL LIKE patterns to exclude usernames (e.g. '%%myapp%%'). Can be repeated.",
    ),
) -> None:
    """
    Discovers all users in the db who can log in, saves them in the config file,
    then revokes their permission to log in. Use this command to ensure that all
    writes to the source database have been stopped before syncing sequence values
    and tables without primary keys.

    Always excludes built-in service accounts (pglogical, rdsadmin, monitoring, etc.).
    Use --exclude-user to exclude additional specific usernames.
    Use --exclude-pattern to exclude usernames matching SQL LIKE patterns (e.g. '%%repuser%%').

    Example:
        belt revoke-logins testdc --exclude-user datadog --exclude-pattern '%%repuser%%'
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "login.src")

    # Typer Option defaults are OptionInfo objects when called programmatically;
    # coerce to plain lists so downstream iteration always works.
    if not isinstance(exclude_users, list):
        exclude_users = []
    if not isinstance(exclude_patterns, list):
        exclude_patterns = []

    all_exclude_users = list(conf.exclude_users or []) + exclude_users
    all_exclude_patterns = list(conf.exclude_patterns or []) + exclude_patterns
    compiled_patterns = [_like_to_regex(p) for p in all_exclude_patterns]
    extra_exclude = all_exclude_users

    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        save_task = None
        if conf.src.other_users is None:
            await _populate_logins(conf.src, pool, logger)
            save_task = asyncio.create_task(conf.save())

        to_disable = []
        if conf.src.owner_user.name != conf.src.root_user.name:
            if not _is_excluded(
                conf.src.owner_user.name, extra_exclude, compiled_patterns
            ):
                to_disable.append(conf.src.owner_user.name)

        if conf.src.other_users is not None:
            to_disable += [
                u.name
                for u in conf.src.other_users
                if u.name not in NO_DISABLE
                and not _is_excluded(u.name, extra_exclude, compiled_patterns)
            ]

        if extra_exclude or compiled_patterns:
            excluded_names = set(NO_DISABLE) | set(extra_exclude)
            all_names = [conf.src.owner_user.name] + [
                u.name for u in (conf.src.other_users or [])
            ]
            skipped = [
                n
                for n in all_names
                if n not in excluded_names
                and n not in to_disable
                and _is_excluded(n, extra_exclude, compiled_patterns)
            ]
            if skipped:
                logger.info(
                    f"Excluded from revocation by --exclude-user/--exclude-pattern: "
                    f"{', '.join(skipped)}"
                )

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
