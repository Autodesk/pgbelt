"""
Tests that run_with_configs runs a schema_names config once per schema, unless the
command acts on the whole database.
"""

from unittest.mock import patch

import pytest
from pgbelt.cmd.helpers import run_with_configs


async def _record(config_future):
    """Return the schemas and subscription name of the migration it ran for."""
    conf = await config_future
    return (conf.schema_names, conf.forward_subscription)


@pytest.mark.asyncio
@pytest.mark.parametrize("expand", [True, False])
async def test_schema_names_config_runs_once_per_schema(config, expand):
    config.schema_names = ["app", "audit"]
    command = run_with_configs(_record, expand_schemas=expand)

    async def fake_get_config(*args, **kwargs):
        return config

    with patch("pgbelt.cmd.helpers.get_config_async", fake_get_config):
        results = await command("test-dc", "test-db")

    if expand:
        assert sorted(r[0] for r in results) == [["app"], ["audit"]]
        assert len({r[1] for r in results}) == 2
    else:
        assert results == [(["app", "audit"], "pg1_pg2")]


@pytest.mark.asyncio
async def test_single_schema_config_runs_once(config):
    command = run_with_configs(_record)

    async def fake_get_config(*args, **kwargs):
        return config

    with patch("pgbelt.cmd.helpers.get_config_async", fake_get_config):
        results = await command("test-dc", "test-db")

    assert results == [(["public"], "pg1_pg2")]

