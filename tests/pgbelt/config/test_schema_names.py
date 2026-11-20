import pytest
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import migration_id
from pydantic import ValidationError


def _config(**kwargs) -> DbupgradeConfig:
    return DbupgradeConfig(db="testdb", dc="testdc", **kwargs)


def test_single_schema_config_keeps_legacy_names():
    conf = _config(schema_name="app")

    assert conf.expand_schemas() == [conf]
    assert conf.replication_set == "pgbelt"
    assert conf.forward_subscription == "pg1_pg2"
    assert conf.back_subscription == "pg2_pg1"
    assert conf.schema_dump_key == "testdb"


def test_schema_names_expand_to_one_migration_per_schema():
    migrations = _config(schema_names=["app", "audit"]).expand_schemas()

    assert [m.schema_name for m in migrations] == ["app", "audit"]
    assert all(m.schema_names is None for m in migrations)
    assert len({m.migration_id for m in migrations}) == 2


def test_expanded_migrations_get_unique_pglogical_names():
    app, audit = _config(schema_names=["app", "audit"]).expand_schemas()

    for m in (app, audit):
        assert m.replication_set == f"pgbelt_{m.migration_id}"
        assert m.forward_subscription == f"pg1_pg2_{m.migration_id}"
        assert m.back_subscription == f"pg2_pg1_{m.migration_id}"
        assert m.schema_dump_key == f"testdb/{m.schema_name}"
    assert app.replication_set != audit.replication_set
    assert app.forward_subscription != audit.forward_subscription


def test_migration_id_is_stable_and_depends_on_its_properties():
    base = migration_id("testdc", "testdb", "app")

    assert base == migration_id("testdc", "testdb", "app")
    assert len(base) == 8
    assert base != migration_id("testdc", "testdb", "audit")
    assert base != migration_id("testdc", "otherdb", "app")
    assert base != migration_id("testdc", "testdb", "app", ["users"])
    assert migration_id("testdc", "testdb", "app", ["a", "b"]) == migration_id(
        "testdc", "testdb", "app", ["b", "a"]
    )


def test_expanded_names_fit_postgres_identifiers():
    (m,) = _config(schema_names=["app"]).expand_schemas()

    assert len(m.replication_set) < 64
    assert len(m.forward_subscription) < 64


def test_schema_names_survive_a_save_round_trip():
    conf = _config(schema_names=["app", "audit"])

    loaded = DbupgradeConfig.model_validate_json(conf.model_dump_json())

    assert loaded.expand_schemas() == conf.expand_schemas()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"schema_names": []},
        {"schema_names": ["app", ""]},
        {"schema_names": ["app", "app"]},
        {"schema_names": ["app"], "tables": ["users"]},
        {"schema_names": ["app"], "sequences": ["users_id_seq"]},
    ],
)
def test_invalid_schema_names_are_rejected(kwargs):
    with pytest.raises(ValidationError):
        _config(**kwargs)
