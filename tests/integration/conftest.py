from os import environ
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User

import pytest


@pytest.fixture(scope="session")
def setup_db_upgrade_config():
    """
    To set up a config for testing -- we form the Python object then save it to disk,
    since pgbelt will check disk for that same config when running commands.
    """

    test_db_upgrade_config = DbupgradeConfig()

    test_db_upgrade_config.db = "integrationtestdb"
    test_db_upgrade_config.dc = "integrationtest-datacenter"

    test_db_upgrade_config.src = DbConfig()
    test_db_upgrade_config.src.host = environ["TEST_PG_SRC_HOST"]
    test_db_upgrade_config.src.ip = environ["TEST_PG_SRC_IP"]
    test_db_upgrade_config.src.db = environ["TEST_PG_SRC_DB"]
    test_db_upgrade_config.src.port = environ["TEST_PG_SRC_PORT"]
    test_db_upgrade_config.src.root_user = User()
    test_db_upgrade_config.src.root_user.name = environ["TEST_PG_SRC_ROOT_USERNAME"]
    test_db_upgrade_config.src.root_user.pw = environ["TEST_PG_SRC_ROOT_PASSWORD"]
    test_db_upgrade_config.src.owner_user = User()
    # These will not be made in the Postgres containers used for testing, so we will define them
    test_db_upgrade_config.src.owner_user.name = "owner"
    test_db_upgrade_config.src.owner_user.pw = "ownerpassword"
    test_db_upgrade_config.src.pglogical_user = User()
    # These are set in the db by pgbelt anyways
    test_db_upgrade_config.src.pglogical_user.name = "pglogical"
    test_db_upgrade_config.src.pglogical_user.pw = "pglogicalpassword"

    test_db_upgrade_config.dst = DbConfig()
    test_db_upgrade_config.dst.host = environ["TEST_PG_DST_HOST"]
    test_db_upgrade_config.dst.ip = environ["TEST_PG_DST_IP"]
    test_db_upgrade_config.dst.db = environ["TEST_PG_DST_DB"]
    test_db_upgrade_config.dst.port = environ["TEST_PG_DST_PORT"]
    test_db_upgrade_config.dst.root_user = User()
    test_db_upgrade_config.dst.root_user.name = environ["TEST_PG_DST_ROOT_USERNAME"]
    test_db_upgrade_config.dst.root_user.pw = environ["TEST_PG_DST_ROOT_PASSWORD"]
    test_db_upgrade_config.dst.owner_user = User()
    # These will not be made in the Postgres containers used for testing, so we will define them
    test_db_upgrade_config.dst.owner_user.name = "owner"
    test_db_upgrade_config.dst.owner_user.pw = "password"
    test_db_upgrade_config.dst.pglogical_user = User()
    # These are set in the db by pgbelt anyways
    test_db_upgrade_config.dst.pglogical_user.name = "pglogical"
    test_db_upgrade_config.dst.pglogical_user.pw = "pglogicalpassword"

    # TODO: Save to disk

    # TODO: Make an owner user in the test containers

    # TODO: Make a DB

    # TODO: Make a fake schema with data


# def teardown():
# TODO: Is this needed? Destroying the containers solves this issues

# TODO: Delete the config that was saved to disk by the setup
