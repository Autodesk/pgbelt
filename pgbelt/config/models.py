from __future__ import annotations

from hashlib import sha256
from os.path import join
from typing import Optional  # noqa: F401 # Needed until tiangolo/typer#522 is fixed)

from aiofiles import open as aopen
from aiofiles.os import remove
from pgbelt.util import get_logger
from pgbelt.util.asyncfuncs import makedirs
from pydantic import BaseModel
from pydantic import PrivateAttr
from pydantic import ValidationError
from pydantic import field_validator
from pydantic import model_validator
from urllib.parse import quote


def config_dir(db: str, dc: str) -> str:
    return f"configs/{dc}/{db}"


def config_file(db: str, dc: str) -> str:
    return join(config_dir(db, dc), "config.json")


def not_empty(v) -> Optional[str]:
    if v == "":
        raise ValueError
    return v


def reject_schema_name(data):
    """
    schema_name was replaced by schema_names. Fail loudly on configs that still set
    it, instead of silently migrating the default schema.
    """
    if isinstance(data, dict) and "schema_name" in data:
        raise ValueError(
            'schema_name was replaced by schema_names, for example "schema_names": ["public"]'
        )
    return data


def migration_id(
    dc: str, db: str, schema: str, tables: Optional[list[str]] = None
) -> str:
    """
    A short hash unique to the properties of a migration: the database pair, the
    schema and the targeted tables.
    """
    key = "/".join([dc, db, schema, ",".join(sorted(tables or []))])
    return sha256(key.encode()).hexdigest()[:8]


class User(BaseModel):
    """
    Represents a user in a postgres db.

    name: str The user name.
    pw: str The user's password. Only required for users pgbelt needs to log in as.
    """

    name: str
    pw: Optional[str] = None

    _not_empty = field_validator("name", "pw")(not_empty)


class DbConfig(BaseModel):
    """
    Represents a postgres db instance.

    host: str The hostname of this instance.
    ip: str The ip of this instance. Instance IPs must be reachable from one another.
    db: str The dbname to operate on. If you want to migrate multiple dbs in a single instance set up a separate config.
    port: str The port to connect to.
    root_user: User A superuser. Usually the postgres user.
    owner_user: User A user who owns all the data in the your specified schema or who has equivalent permissions. # noqa: RST301
                     This user will end up owning all the data if this is describing the target instance.
    pglogical_user: User A user for use with pglogical. Will be created if it does not exist.
    """

    host: str
    ip: str
    db: str
    port: str
    root_user: User
    owner_user: User
    pglogical_user: User

    _not_empty = field_validator("host", "ip", "db", "port")(not_empty)

    @field_validator("root_user", "owner_user", "pglogical_user")
    def has_password(cls, v) -> User:  # noqa: N805
        if not v.pw:
            raise ValueError
        return v

    @property
    def root_dsn(self) -> str:
        return f"hostaddr={self.ip} port={self.port} dbname={self.db} user={self.root_user.name} password={self.root_user.pw}"

    @property
    def owner_dsn(self) -> str:
        return f"hostaddr={self.ip} port={self.port} dbname={self.db} user={self.owner_user.name} password={self.owner_user.pw}"

    @property
    def pglogical_dsn(self) -> str:
        return f"hostaddr={self.ip} port={self.port} dbname={self.db} user={self.pglogical_user.name} password={self.pglogical_user.pw}"

    @property
    def root_uri(self) -> str:
        password = quote(
            self.root_user.pw
        )  # https://github.com/encode/databases/issues/145#issuecomment-1303792343 need this to handle special characters
        return f"postgresql://{self.root_user.name}:{password}@{self.ip}:{self.port}/{self.db}"

    @property
    def owner_uri(self) -> str:
        password = quote(
            self.owner_user.pw
        )  # https://github.com/encode/databases/issues/145#issuecomment-1303792343 need this to handle special characters
        return f"postgresql://{self.owner_user.name}:{password}@{self.ip}:{self.port}/{self.db}"

    @property
    def pglogical_uri(self) -> str:
        password = quote(
            self.pglogical_user.pw
        )  # https://github.com/encode/databases/issues/145#issuecomment-1303792343 need this to handle special characters
        return f"postgresql://{self.pglogical_user.name}:{password}@{self.ip}:{self.port}/{self.db}"


class DbupgradeConfig(BaseModel):
    """
    Represents a migration to be performed.

    db: str A name used to identify this specific database pair. Used in cli commands.
    dc: str A name used to identify the environment this database pair is in. Used in cli commands.
    src: DbConfig The database we are moving data out of.
    dst: DbConfig The database we are moving data into.
    tables: Optional[list[str]] A list of tables to replicate. If not provided all tables in the named schema will be replicated.
    sequences: Optional[list[str]] A list of sequences to replicate. If not provided all sequences in the named schema will be replicated.
    schema_names: list[str] The schemas to migrate. Defaults to ["public"]. With several schemas, pgbelt runs one migration per schema in parallel. tables and sequences need a single schema.
    migration_id: Optional[str] Set by pgbelt on each per-schema migration expanded from several schema_names. Used to give its pglogical objects unique names.
    exclude_users: Optional[list[str]] Usernames to exclude from connection counts and login revocation.
    exclude_patterns: Optional[list[str]] SQL LIKE patterns to exclude usernames (e.g. '%%repuser%%').
    """

    db: str
    dc: str
    src: Optional[DbConfig] = None
    dst: Optional[DbConfig] = None
    tables: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    schema_names: list[str] = ["public"]
    migration_id: Optional[str] = None
    exclude_users: Optional[list[str]] = None
    exclude_patterns: Optional[list[str]] = None

    _copies_extension_tables: bool = PrivateAttr(default=True)

    _not_empty = field_validator("db", "dc")(not_empty)

    _reject_schema_name = model_validator(mode="before")(reject_schema_name)

    @model_validator(mode="after")
    def check_schema_names(self) -> DbupgradeConfig:
        if not self.schema_names or "" in self.schema_names:
            raise ValueError("schema_names must list at least one non-empty schema")
        if len(set(self.schema_names)) != len(self.schema_names):
            raise ValueError("schema_names must not contain duplicates")
        if len(self.schema_names) > 1 and (self.tables or self.sequences):
            raise ValueError(
                "tables and sequences need a single schema in schema_names"
            )
        return self

    @property
    def schema_name(self) -> str:
        """
        The schema of a single-schema migration. Configs with several schema_names
        are split with expand_schemas() before any command uses this.
        """
        if len(self.schema_names) != 1:
            raise ValueError(
                f"{self.db} lists several schemas, expand it with expand_schemas() first"
            )
        return self.schema_names[0]

    def expand_schemas(self) -> list[DbupgradeConfig]:
        """
        Split a config with several schema_names into one migration per schema. Each
        one gets a migration_id so its pglogical objects don't collide with the others
        in the same database. A single-schema config is returned unchanged and keeps
        the default pglogical names.
        """
        if len(self.schema_names) == 1:
            return [self]
        migrations = [
            self.model_copy(
                update={
                    "schema_names": [schema],
                    "migration_id": migration_id(self.dc, self.db, schema, self.tables),
                }
            )
            for schema in self.schema_names
        ]
        for i, migration in enumerate(migrations):
            migration._copies_extension_tables = i == 0
        return migrations

    @property
    def copies_extension_tables(self) -> bool:
        """
        Extension-owned tables belong to the whole database, so of the migrations
        expanded from several schema_names only the first schema's copies them.
        """
        return self._copies_extension_tables

    def _pglogical_name(self, base: str) -> str:
        return f"{base}_{self.migration_id}" if self.migration_id else base

    @property
    def replication_set(self) -> str:
        return self._pglogical_name("pgbelt")

    @property
    def forward_subscription(self) -> str:
        return self._pglogical_name("pg1_pg2")

    @property
    def back_subscription(self) -> str:
        return self._pglogical_name("pg2_pg1")

    @property
    def schema_dump_key(self) -> str:
        """
        Directory key for schema dump files. Per-schema migrations get their own
        subdirectory so parallel dumps don't overwrite each other.
        """
        return f"{self.db}/{self.schema_name}" if self.migration_id else self.db

    @property
    def file(self) -> str:
        return config_file(self.db, self.dc)

    @property
    def dir(self) -> str:
        return config_dir(self.db, self.dc)

    async def save(self):
        """
        Write the configuration out to disk
        """
        logger = get_logger(self.db, self.dc, "config")
        logger.debug("Caching config to disk...")

        try:
            await makedirs(self.dir)
        except FileExistsError:
            pass

        try:
            await remove(self.file)
        except FileNotFoundError:
            pass

        async with aopen(self.file, "w") as f:
            await f.write(self.model_dump_json(indent=4))

        logger.info("Cached config to disk.")

    @classmethod
    async def load(cls, db: str, dc: str) -> Optional[DbupgradeConfig]:
        """
        Load the specified configuration from disk if present.
        If the existing config is invalid or does not exist return None.
        """
        logger = get_logger(db, dc, "config")
        logger.debug("Trying to load cached config...")

        try:
            async with aopen(config_file(db, dc), "r") as f:
                raw = await f.read()
        except FileNotFoundError:
            logger.info("No cached config available")
            return None

        try:
            out = cls.model_validate_json(raw)
        except ValidationError:
            logger.info("Cached config was not a valid DbupgradeConfig")
            return None

        logger.info("Found cached config.")

        return out
