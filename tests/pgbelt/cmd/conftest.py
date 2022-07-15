from os import environ
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import User

import pytest


def pytest_collectstart():
    environ["CI"] = "true"


@pytest.fixture()
def config():
    testconf = DbupgradeConfig(
        db="test-db",
        dc="test-dc",
        src=DbConfig(
            host="pgbelt-unit-test-src.pgbelt.fake",
            ip="192.168.0.11",
            db="testdbsrc",
            port="5432",
            root_user=User(name="fake_src_root_username", pw="fake_src_root_password"),
            owner_user=User(
                name="fake_src_owner_username", pw="fake_src_owner_password"
            ),
            pglogical_user=User(
                name="fake_src_pgl_username", pw="fake_src_pgl_password"
            ),
        ),
        dst=DbConfig(
            host="pgbelt-unit-test-dst.pgbelt.fake",
            ip="192.168.0.12",
            db="testdbdst",
            port="5432",
            root_user=User(name="fake_dst_root_username", pw="fake_dst_root_password"),
            owner_user=User(
                name="fake_dst_owner_username", pw="fake_dst_owner_password"
            ),
            pglogical_user=User(
                name="fake_dst_pgl_username", pw="fake_dst_pgl_password"
            ),
        ),
    )
    yield testconf
