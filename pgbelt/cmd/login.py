import re
from collections.abc import Awaitable

from asyncpg import create_pool
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util import get_logger
from pgbelt.util.postgres import disable_login_users
from pgbelt.util.postgres import enable_login_users
from pgbelt.util.postgres import get_login_users
from pgbelt.util.postgres import get_nologin_users
from typer import Option


# TODO this should be configurable, add a PgbeltConfig or something
NO_DISABLE = [
    "pglogical",
    "postgres",
    "rdsadmin",
    "rdsrepladmin",
    "rdstopmgr",
    "rdswriteforwarduser",
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


def _build_exclusions(
    conf: DbupgradeConfig,
    exclude_users: list[str],
    exclude_patterns: list[str],
) -> tuple[list[str], list[re.Pattern]]:
    """Merge config-level and CLI-level exclusions into a single filter set.

    Returns (exclude_user_list, compiled_pattern_list).
    """
    # Typer Option defaults are OptionInfo objects when called programmatically;
    # coerce to plain lists so downstream iteration always works.
    if not isinstance(exclude_users, list):
        exclude_users = []
    if not isinstance(exclude_patterns, list):
        exclude_patterns = []

    merged_users = list(conf.exclude_users or []) + exclude_users
    merged_patterns = list(conf.exclude_patterns or []) + exclude_patterns
    return merged_users, [_like_to_regex(p) for p in merged_patterns]


def _filter_roles(
    candidates: list[str],
    skip_set: set[str],
    exclude_users: list[str],
    exclude_patterns: list[re.Pattern],
) -> list[str]:
    """Return candidates that are not in skip_set and not excluded."""
    return [
        name
        for name in candidates
        if name not in skip_set
        and not _is_excluded(name, exclude_users, exclude_patterns)
    ]


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
    Discovers all users who can log in and revokes their permission.
    Stateless — queries pg_roles each time rather than caching a user list.

    Always excludes built-in service accounts (pglogical, rdsadmin, monitoring, etc.).
    Use --exclude-user to exclude additional specific usernames.
    Use --exclude-pattern to exclude usernames matching SQL LIKE patterns (e.g. '%%repuser%%').

    Example:
        belt revoke-logins testdc --exclude-user datadog --exclude-pattern '%%repuser%%'
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "login.src")

    merged_users, compiled_patterns = _build_exclusions(
        conf, exclude_users, exclude_patterns
    )
    skip_set = set(NO_DISABLE) | {conf.src.root_user.name}

    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        login_roles = await get_login_users(pool, logger)
        to_disable = _filter_roles(
            login_roles, skip_set, merged_users, compiled_patterns
        )

        if to_disable:
            await disable_login_users(pool, to_disable, logger)
        else:
            logger.info("No roles to revoke.")


@run_with_configs(skip_dst=True)
async def restore_logins(
    config_future: Awaitable[DbupgradeConfig],
    exclude_users: list[str] = Option(
        [],
        "--exclude-user",
        "-e",
        help="Additional usernames to exclude from restoration (can be repeated).",
    ),
    exclude_patterns: list[str] = Option(
        [],
        "--exclude-pattern",
        "-p",
        help="SQL LIKE patterns to exclude usernames (e.g. '%%myapp%%'). Can be repeated.",
    ),
) -> None:
    """
    Discovers all roles that currently have NOLOGIN and re-enables login for
    them, excluding built-in service accounts and any roles specified via
    --exclude-user / --exclude-pattern.

    This is stateless — it does not rely on a previously saved user list.
    Intended to be used after revoke-logins in case a rollback is required.
    """
    conf = await config_future
    logger = get_logger(conf.db, conf.dc, "login.src")

    merged_users, compiled_patterns = _build_exclusions(
        conf, exclude_users, exclude_patterns
    )
    skip_set = set(NO_DISABLE) | {conf.src.root_user.name}

    async with create_pool(conf.src.root_uri, min_size=1) as pool:
        nologin_roles = await get_nologin_users(pool, logger)
        to_enable = _filter_roles(
            nologin_roles, skip_set, merged_users, compiled_patterns
        )

        if to_enable:
            await enable_login_users(pool, to_enable, logger)
        else:
            logger.info("No roles to restore.")


COMMANDS = [
    revoke_logins,
    restore_logins,
]
