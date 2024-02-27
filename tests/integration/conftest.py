from os import environ
from sys import argv
from shutil import rmtree

import pytest
import pytest_asyncio
import asyncio
from asyncpg import create_pool
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User


async def _create_dbupgradeconfig(
    non_public_schema: bool = False, exodus: bool = False
) -> DbupgradeConfig:
    """
    Function for creating a DbupgradeConfig object for testing.
    We also save it to disk since the pgbelt commands will look for it there.

    If you want to test with a non-public schema, set non_public_schema to True.
    If you want to test the migration of a subset of tables, set exodus to True.
    """

    config = DbupgradeConfig(
        db="testdb",
        dc="testdc",
        schema_name="non_public_schema" if non_public_schema else "public",
        tables=["users2"] if exodus else [],
        sequences=["users2_id_seq"] if exodus else [],
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


async def _prepare_databases(config: DbupgradeConfig, non_public_schema: bool = False):
    """
    Given the root URIs for the source and destination databases:
    1. Create the owner user on both databases
    2. Create the Postgres DB on both databases
    3. Load the test data into the source database
    """

    # Load test data and schema SQL
    with open("tests/integration/files/test_schema_data.sql") as f:
        test_schema_data = f.read()

    # Just replace all the `public.` with `non_public_schema.` if non_public_schema is True
    if non_public_schema:
        test_schema_data = test_schema_data.replace("public.", "non_public_schema.")

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
            f"CREATE DATABASE src WITH OWNER = {config.src.owner_user.name}"
        ),
        dst_root_user_root_db_pool.execute(
            f"CREATE DATABASE dst WITH OWNER = {config.dst.owner_user.name}"
        ),
    )

    src_owner_user_logical_db_pool, dst_owner_user_logical_db_pool = (
        await asyncio.gather(
            create_pool(config.src.owner_uri, min_size=1),
            create_pool(config.dst.owner_uri, min_size=1),
        )
    )

    # Create the non_public_schema if non_public_schema is True
    if non_public_schema:
        await asyncio.gather(
            src_owner_user_logical_db_pool.execute(f"CREATE SCHEMA non_public_schema"),
            dst_owner_user_logical_db_pool.execute(f"CREATE SCHEMA non_public_schema"),
        )
        await asyncio.gather(
            src_owner_user_logical_db_pool.execute(
                f"GRANT CREATE ON SCHEMA non_public_schema TO {config.src.owner_user.name}"
            ),
            dst_owner_user_logical_db_pool.execute(
                f"GRANT CREATE ON SCHEMA non_public_schema TO {config.dst.owner_user.name}"
            ),
        )

    # With the db made, load data into src
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
async def setup_db_upgrade_config_public_schema():
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
    rmtree("configs/testdc")
    rmtree("schemas/")


@pytest.mark.asyncio
@pytest_asyncio.fixture
async def setup_db_upgrade_config_non_public_schema():
    """
    Same as above, but with a non-public schema.
    """

    # Create the config
    test_db_upgrade_config = await _create_dbupgradeconfig(non_public_schema=True)

    # Prepare the databases
    await _prepare_databases(test_db_upgrade_config, non_public_schema=True)

    yield test_db_upgrade_config

    # Clear out all data and stuff in the database containers :shrug:
    await _empty_out_database(test_db_upgrade_config)

    # Delete the config that was saved to disk by the setup
    rmtree("configs/testdc")
    rmtree("schemas/")


@pytest.mark.asyncio
@pytest_asyncio.fixture
async def setup_db_upgrade_config_public_schema_exodus():
    """
    Fixture for preparing the test databases and creating a DbupgradeConfig object.
    This fixture will also clean up after the test (removing local files and tearing down against the DBs).
    """

    # Create the config
    test_db_upgrade_config = await _create_dbupgradeconfig(exodus=True)

    # Prepare the databases
    await _prepare_databases(test_db_upgrade_config)

    yield test_db_upgrade_config

    # Clear out all data and stuff in the database containers :shrug:
    await _empty_out_database(test_db_upgrade_config)

    # Delete the config that was saved to disk by the setup
    rmtree("configs/testdc")
    rmtree("schemas/")


@pytest.mark.asyncio
@pytest_asyncio.fixture
async def setup_db_upgrade_config_non_public_schema_exodus():
    """
    Same as the base fixture, but with a non-public schema and exodus-style (moving a subset of data).
    """

    # Create the config
    test_db_upgrade_config = await _create_dbupgradeconfig(
        non_public_schema=True, exodus=True
    )

    # Prepare the databases
    await _prepare_databases(test_db_upgrade_config, non_public_schema=True)

    yield test_db_upgrade_config

    # Clear out all data and stuff in the database containers :shrug:
    await _empty_out_database(test_db_upgrade_config)

    # Delete the config that was saved to disk by the setup
    rmtree("configs/testdc")
    rmtree("schemas/")


# This is a hacky way of doing it, but I don't want to duplicate code.
# If this code is called directly, we can use it to set up the databases and create the config for
# local interactive testing.

# This will create the datasets.
if __name__ == "__main__":

    # Check if any flags were passed using argv
    if "--non-public-schema" in argv:
        print("Creating non-public schema dataset...")
        non_public_schema = True
    else:
        print("Creating public schema dataset...")
        non_public_schema = False

    config = asyncio.run(_create_dbupgradeconfig(non_public_schema))
    asyncio.run(_prepare_databases(config, non_public_schema))

    print("Local databases are ready for local testing!")
