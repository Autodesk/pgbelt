from os import environ
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User

import pytest
import pytest_asyncio
from asyncpg import create_pool


@pytest.mark.asyncio
@pytest_asyncio.fixture
async def setup_db_upgrade_config():
    """
    To set up a config for testing -- we form the Python object then save it to disk,
    since pgbelt will check disk for that same config when running commands.
    """

    test_db_upgrade_config = DbupgradeConfig(
        db="integrationtestdb",
        dc="integrationtest-datacenter",
    )

    test_db_upgrade_config.src = DbConfig(
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

    test_db_upgrade_config.dst = DbConfig(
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
    await test_db_upgrade_config.save()

    # Make src root URI with root dbname not the one to be made
    src_root_uri_with_root_db = test_db_upgrade_config.src.root_uri.replace(
        f"{test_db_upgrade_config.src.port}/{test_db_upgrade_config.src.db}",
        f"{test_db_upgrade_config.src.port}/postgres",
    )

    # Load test data and schema SQL
    with open("tests/integration/files/test_schema_data.sql") as f:
        test_schema_data = f.read()

    # Make the following in the src container: owner user, db
    async with create_pool(root_uri_with_root_db, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE ROLE {test_db_upgrade_config.src.owner_user.name} LOGIN PASSWORD '{test_db_upgrade_config.src.owner_user.pw}'",
            )
            await conn.execute("CREATE DATABASE src")

    # With the db made, load data into src
    async with create_pool(test_db_upgrade_config.src.owner_uri, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE ROLE {test_db_upgrade_config.src.owner_user.name} LOGIN PASSWORD '{test_db_upgrade_config.src.owner_user.pw}'",
            )
            await conn.execute("CREATE DATABASE src")
            await conn.execute(test_schema_data)

    # Make dst root URI with root dbname not the one to be made
    dst_root_uri_with_root_db = test_db_upgrade_config.dst.root_uri.replace(
        f"{test_db_upgrade_config.dst.port}/{test_db_upgrade_config.dst.db}",
        f"{test_db_upgrade_config.dst.port}/postgres",
    )

    # Make the following in the dst container: owner user, db
    async with create_pool(dst_root_uri_with_root_db, min_size=1) as pool:
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE ROLE {test_db_upgrade_config.dst.owner_user.name} LOGIN PASSWORD '{test_db_upgrade_config.dst.owner_user.pw}'",
            )
            await conn.execute("CREATE DATABASE src")


# def teardown():
# TODO: Is this needed? Destroying the containers solves this issues

# TODO: Delete the config that was saved to disk by the setup
