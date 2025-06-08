from os import environ
from shutil import rmtree

import pytest_asyncio
import asyncio
from asyncpg import create_pool
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User


async def _create_dbupgradeconfigs() -> dict[str, DbupgradeConfig]:
    """
    Function for creating DbupgradeConfig objects for testing.
    We also save it to disk since the pgbelt commands will look for it there.

    This will create 4 sets of DBs: public vs non-public schema, and exodus-style vs full migration.
    """

    # Set the common kwargs at the DBUpgradeConfig level
    db_upgrade_config_kwargs = {
        "dc": "testdc",
    }

    # Set the common config kwargs for the individual DBs
    # We set many of the args here in the actual DB containers, so we don't need to pull these vars out to docker-compose.
    common_db_config_kwargs = {
        "host": "localhost",
        "port": "5432",
        # This is the default credential for the admin user in the Postgres containers used for testing.
        "root_user": User(
            name="postgres",
            pw="postgres",
        ),
        # We will create the owner_user in the DBs via the integration test setup.
        # Due to issue #440, we're adding a special character to the password to ensure this still works.
        "owner_user": User(name="owner", pw="owner#password"),
        "pglogical_user": User(name="pglogical", pw="pglogicalpassword"),
        "db": "testdb",
    }

    # We're treating DB pairs as sets here.
    sets = [
        "public-full",
        "nonpublic-full",
        "public-exodus",
        "nonpublic-exodus",
    ]

    configs = {}
    for s in sets:
        db_upgrade_config_kwargs["db"] = f"testdb-{s}"
        db_upgrade_config_kwargs["schema_name"] = (
            "non_public_schema" if "nonpublic" in s else "public"
        )
        db_upgrade_config_kwargs["tables"] = (
            ["UsersCapital", "existingSomethingIds"] if "exodus" in s else None
        )
        db_upgrade_config_kwargs["sequences"] = (
            ["userS_id_seq"] if "exodus" in s else None
        )
        config = DbupgradeConfig(**db_upgrade_config_kwargs)

        # The IP addresses are set in the docker-compose file, so we can pull them out of the environment. They follow the following pattern:
        # (NON)PUBLIC_<FULL/EXODUS>_<SRC/DST>_IP
        config.src = DbConfig(
            ip=environ[f"{s.split('-')[0].upper()}_{s.split('-')[1].upper()}_SRC_IP"],
            **common_db_config_kwargs,
        )
        config.dst = DbConfig(
            ip=environ[f"{s.split('-')[0].upper()}_{s.split('-')[1].upper()}_DST_IP"],
            **common_db_config_kwargs,
        )

        # Save the config to disk
        await config.save()
        configs[s] = config

    return configs


async def _prepare_databases(configs: dict[str, DbupgradeConfig]) -> None:
    """
    Given a dict of various configs for database pairs, prepare the following:
    1. Create the owner user on both databases
    2. Create the Postgres DB on both databases
    3. If the schema is non-public, create the schema on both databases
    4. Load the test data into the source database
    """

    # Load test data and schema SQL
    with open("tests/integration/files/test_schema_data.sql") as f:
        base_test_schema_data = f.read()

    for config in configs.values():

        # Get the root connections to the root DBs
        src_root_uri_with_root_db, dst_root_uri_with_root_db = _root_uris(config)
        src_root_user_root_db_pool, dst_root_user_root_db_pool = await asyncio.gather(
            create_pool(src_root_uri_with_root_db, min_size=1),
            create_pool(dst_root_uri_with_root_db, min_size=1),
        )

        # Create the owner user
        await asyncio.gather(
            src_root_user_root_db_pool.execute(
                f"CREATE ROLE {config.src.owner_user.name} LOGIN PASSWORD '{config.src.owner_user.pw}'",
            ),
            dst_root_user_root_db_pool.execute(
                f"CREATE ROLE {config.dst.owner_user.name} LOGIN PASSWORD '{config.dst.owner_user.pw}'",
            ),
        )

        # Create the databases
        await asyncio.gather(
            src_root_user_root_db_pool.execute(
                f"CREATE DATABASE {config.src.db} WITH OWNER = {config.src.owner_user.name}"
            ),
            dst_root_user_root_db_pool.execute(
                f"CREATE DATABASE {config.dst.db} WITH OWNER = {config.dst.owner_user.name}"
            ),
        )

        src_owner_user_logical_db_pool, dst_owner_user_logical_db_pool = (
            await asyncio.gather(
                create_pool(config.src.owner_uri, min_size=1),
                create_pool(config.dst.owner_uri, min_size=1),
            )
        )

        # Create the non-public schema if the schema_name is not "public"
        if config.schema_name != "public":
            await asyncio.gather(
                src_owner_user_logical_db_pool.execute(
                    f"CREATE SCHEMA {config.schema_name}"
                ),
                dst_owner_user_logical_db_pool.execute(
                    f"CREATE SCHEMA {config.schema_name}"
                ),
            )
            await asyncio.gather(
                src_owner_user_logical_db_pool.execute(
                    f"GRANT CREATE ON SCHEMA {config.schema_name} TO {config.src.owner_user.name}"
                ),
                dst_owner_user_logical_db_pool.execute(
                    f"GRANT CREATE ON SCHEMA {config.schema_name} TO {config.dst.owner_user.name}"
                ),
            )

        # With the db made, load data into src
        test_schema_data = base_test_schema_data

        # If we're testing with a non-public schema, we need to replace the schema name in our schema template.
        if config.schema_name != "public":
            test_schema_data = test_schema_data.replace(
                "public.", f"{config.schema_name}."
            )

        await asyncio.gather(
            src_owner_user_logical_db_pool.execute(test_schema_data),
        )


def _root_uris(config: DbupgradeConfig) -> tuple[str, str]:
    """
    Given a DbupgradeConfig object, return the root URIs for the source and destination databases.
    """

    # Make src root URI with root dbname not the one to be made
    src_root_uri_with_root_db = config.src.root_uri.replace(
        f"{config.src.port}/{config.src.db}",
        f"{config.src.port}/postgres",
    )

    # Make dst root URI with root dbname not the one to be made
    dst_root_uri_with_root_db = config.dst.root_uri.replace(
        f"{config.dst.port}/{config.dst.db}",
        f"{config.dst.port}/postgres",
    )

    return src_root_uri_with_root_db, dst_root_uri_with_root_db


async def _empty_out_databases(configs: dict[str, DbupgradeConfig]) -> None:
    """
    This code will DROP the databases specified in the config,
    DROP the owner role specified in the config and any permissions with it.
    """

    for config in configs.values():

        # Get the root URIs
        src_root_uri_with_root_db, dst_root_uri_with_root_db = _root_uris(config)

        async with create_pool(src_root_uri_with_root_db, min_size=1) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    f"DROP DATABASE {config.src.db} WITH (FORCE);",
                )
                await conn.execute(
                    f"DROP OWNED BY {config.src.owner_user.name};",
                )
                await conn.execute(
                    f"DROP ROLE {config.src.owner_user.name};",
                )

        async with create_pool(dst_root_uri_with_root_db, min_size=1) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    f"DROP DATABASE {config.dst.db} WITH (FORCE);",
                )
                await conn.execute(
                    f"DROP OWNED BY {config.dst.owner_user.name};",
                )
                await conn.execute(
                    f"DROP ROLE {config.dst.owner_user.name};",
                )


@pytest_asyncio.fixture
async def setup_db_upgrade_configs():
    """
    Fixture for preparing the test databases and creating a DbupgradeConfig object.
    This fixture will also clean up after the test (removing local files and tearing down against the DBs).
    """

    # Create the config
    test_configs = await _create_dbupgradeconfigs()

    # Prepare the databases
    # await _prepare_databases(test_configs)

    yield test_configs

    # Clear out all data and stuff in the database containers :shrug:
    await _empty_out_databases(test_configs)

    # Delete the config that was saved to disk by the setup
    rmtree("configs/testdc")
    rmtree("schemas/")


# This is a hacky way of doing it, but I don't want to duplicate code.
# If this code is called directly, we can use it to set up the databases and create the config for
# local interactive testing.

# This will create the datasets.
if __name__ == "__main__":

    configs = asyncio.run(_create_dbupgradeconfigs())
    asyncio.run(_prepare_databases(configs))

    print("Local databases are ready for local testing!")
