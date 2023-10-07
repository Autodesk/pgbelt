from os import environ
from shutil import rmtree

import pytest
import pytest_asyncio
import asyncio
from asyncpg import create_pool
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User


async def _create_dbupgradeconfig() -> DbupgradeConfig:
    """
    Function for creating a DbupgradeConfig object for testing.
    We also save it to disk since the pgbelt commands will look for it there.
    """

    config = DbupgradeConfig(
        db="integrationtestdb",
        dc="integrationtest-datacenter",
    )

    config.src = DbConfig(
        host=environ["TEST_PG_SRC_HOST"],
        ip=environ["TEST_PG_SRC_IP"],
        db=environ["TEST_PG_SRC_DB"],
        port=environ["TEST_PG_SRC_PORT"],
        root_user=User(
            name=environ["TEST_PG_SRC_ROOT_USERNAME"],
            pw=environ["TEST_PG_SRC_ROOT_PASSWORD"],
        ),
        # Owner will not be made in the Postgres containers used for testing, so we will define them
        owner_user=User(name="owner", pw="ownerpassword"),
        # Pglogical user info is set in the db by pgbelt, so we defined this stuff here
        pglogical_user=User(name="pglogical", pw="pglogicalpassword"),
    )

    config.dst = DbConfig(
        host=environ["TEST_PG_DST_HOST"],
        ip=environ["TEST_PG_DST_IP"],
        db=environ["TEST_PG_DST_DB"],
        port=environ["TEST_PG_DST_PORT"],
        root_user=User(
            name=environ["TEST_PG_DST_ROOT_USERNAME"],
            pw=environ["TEST_PG_DST_ROOT_PASSWORD"],
        ),
        # Owner will not be made in the Postgres containers used for testing, so we will define them
        owner_user=User(name="owner", pw="ownerpassword"),
        # Pglogical user info is set in the db by pgbelt, so we defined this stuff here
        pglogical_user=User(name="pglogical", pw="pglogicalpassword"),
    )

    # Save to disk
    await config.save()

    return config


async def _prepare_databases(config: DbupgradeConfig):
    """
    Given the root URIs for the source and destination databases:
    1. Create the owner user on both databases
    2. Create the Postgres DB on both databases
    3. Load the test data into the source database
    """

    # Get the root URIs
    src_root_uri_with_root_db, dst_root_uri_with_root_db = _root_uris(config)

    # Load test data and schema SQL
    with open("tests/integration/files/test_schema_data.sql") as f:
        test_schema_data = f.read()

    # Make the following in the src container: owner user, db
    async with create_pool(src_root_uri_with_root_db, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE ROLE {config.src.owner_user.name} LOGIN PASSWORD '{config.src.owner_user.pw}'",
            )
            await conn.execute("CREATE DATABASE src")

    # With the db made, load data into src
    async with create_pool(config.src.owner_uri, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(test_schema_data)

    # Make the following in the dst container: owner user, db
    async with create_pool(dst_root_uri_with_root_db, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE ROLE {config.dst.owner_user.name} LOGIN PASSWORD '{config.dst.owner_user.pw}'",
            )
            await conn.execute("CREATE DATABASE dst")


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


async def _empty_out_database(config: DbupgradeConfig) -> None:
    """
    This code will DROP the databases specified in the config,
    DROP the owner role specified in the config and any permissions with it.
    """

    # Get the root URIs
    src_root_uri_with_root_db, dst_root_uri_with_root_db = _root_uris(config)

    async with create_pool(src_root_uri_with_root_db, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"DROP DATABASE src WITH (FORCE);",
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
                f"DROP DATABASE dst WITH (FORCE);",
            )
            await conn.execute(
                f"DROP OWNED BY {config.dst.owner_user.name};",
            )
            await conn.execute(
                f"DROP ROLE {config.dst.owner_user.name};",
            )


@pytest.mark.asyncio
@pytest_asyncio.fixture
async def setup_db_upgrade_config():
    """
    Fixture for preparing the test databases and creating a DbupgradeConfig object.
    This fixture will also clean up after the test (removing local files and tearing down against the DBs).
    """

    # Create the config
    test_db_upgrade_config = await _create_dbupgradeconfig()

    # Prepare the databases
    await _prepare_databases(test_db_upgrade_config)

    yield test_db_upgrade_config

    # Clear out all data and stuff in the database containers :shrug:
    await _empty_out_database(test_db_upgrade_config)

    # Delete the config that was saved to disk by the setup
    rmtree("configs/integrationtest-datacenter")
    rmtree("schemas/")


# This is a hacky way of doing it, but I don't want to duplicate code.
# If this code is called directly, we can use it to set up the databases and create the config for
# local interactive testing.
if __name__ == "__main__":
    config = asyncio.run(_create_dbupgradeconfig())
    asyncio.run(_prepare_databases(config))

    print("Local databases are ready for local testing!")
