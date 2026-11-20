import pytest
from pgbelt.config.models import DbupgradeConfig
from pgbelt.config.models import migration_id
from pydantic import ValidationError


def _config(**kwargs) -> DbupgradeConfig:
    return DbupgradeConfig(db="testdb", dc="testdc", **kwargs)


def test_single_schema_config_keeps_legacy_names():
    conf = _config(schema_names=["app"])

    assert conf.expand_schemas() == [conf]
    assert conf.schema_name == "app"
    assert conf.replication_set == "pgbelt"
    assert conf.forward_subscription == "pg1_pg2"
    assert conf.back_subscription == "pg2_pg1"
    assert conf.schema_dump_key == "testdb"


def test_schema_names_expand_to_one_migration_per_schema():
    migrations = _config(schema_names=["app", "audit"]).expand_schemas()

    assert [m.schema_name for m in migrations] == ["app", "audit"]
    assert [m.schema_names for m in migrations] == [["app"], ["audit"]]
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


def test_only_the_first_schema_copies_extension_tables():
    migrations = _config(schema_names=["app", "audit", "reports"]).expand_schemas()

    assert [m.copies_extension_tables for m in migrations] == [True, False, False]
    assert _config(schema_names=["app"]).copies_extension_tables is True


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
    m = _config(schema_names=["app", "audit"]).expand_schemas()[0]

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
        {"schema_names": ["app", "audit"], "tables": ["users"]},
        {"schema_names": ["app", "audit"], "sequences": ["users_id_seq"]},
    ],
)
def test_invalid_schema_names_are_rejected(kwargs):
    with pytest.raises(ValidationError):
        _config(**kwargs)


def test_schema_names_defaults_to_public():
    conf = _config()

    assert conf.schema_names == ["public"]
    assert conf.schema_name == "public"
    assert conf.expand_schemas() == [conf]


def test_single_schema_can_target_tables_and_sequences():
    conf = _config(schema_names=["app"], tables=["users"], sequences=["users_id_seq"])

    assert conf.expand_schemas() == [conf]
    assert conf.replication_set == "pgbelt"


def test_schema_name_is_rejected_with_a_hint():
    with pytest.raises(ValidationError, match="replaced by schema_names"):
        _config(schema_name="app")


def test_cached_config_with_schema_name_is_rejected():
    raw = (
        _config()
        .model_dump_json()
        .replace('"schema_names":["public"]', '"schema_name":"app"')
    )

    with pytest.raises(ValidationError, match="replaced by schema_names"):
        DbupgradeConfig.model_validate_json(raw)


def test_schema_name_needs_a_single_schema():
    with pytest.raises(ValueError, match="expand_schemas"):
        _config(schema_names=["app", "audit"]).schema_name
