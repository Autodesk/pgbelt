"""Tests for pgbelt per-side resolver contract and backward compat."""

from __future__ import annotations

import asyncio
import json
from logging import getLogger
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from pgbelt.config.models import DbConfig, DbupgradeConfig, User
from pgbelt.config.remote import (
    BaseResolver,
    BaseSideResolver,
    RemoteConfigDefinition,
    RemoteConfigError,
    resolve_remote_config,
)


FAKE_DBCONFIG_SRC = DbConfig(
    host="src.example.com",
    ip="10.0.0.1",
    db="mydb",
    port="5432",
    root_user=User(name="postgres", pw="rootpw"),
    owner_user=User(name="owner", pw="ownerpw"),
    pglogical_user=User(name="pglogical", pw="srcpglpw"),
)

FAKE_DBCONFIG_DST = DbConfig(
    host="dst.example.com",
    ip="10.0.0.2",
    db="mydb",
    port="5432",
    root_user=User(name="admin", pw="rootpw"),
    owner_user=User(name="owner", pw="ownerpw"),
    pglogical_user=User(name="pglogical", pw="dstpglpw"),
)


class FakeSrcResolver(BaseSideResolver):
    app: str
    pgrs_path: str

    async def resolve(self) -> Optional[DbConfig]:
        return FAKE_DBCONFIG_SRC


class FakeDstResolver(BaseSideResolver):
    secret_arn: str

    async def resolve(self) -> Optional[DbConfig]:
        return FAKE_DBCONFIG_DST


class FakeNoneResolver(BaseSideResolver):
    async def resolve(self) -> Optional[DbConfig]:
        return None


class FakeErrorResolver(BaseSideResolver):
    async def resolve(self) -> Optional[DbConfig]:
        raise RemoteConfigError("connection refused")


class FakeLegacyResolver(BaseResolver):
    src_app: str

    async def resolve(self) -> Optional[DbupgradeConfig]:
        return DbupgradeConfig(
            db=self.db, dc=self.dc, src=FAKE_DBCONFIG_SRC, dst=FAKE_DBCONFIG_DST,
        )


class TestRemoteConfigDefinition:
    def test_legacy_path(self):
        d = RemoteConfigDefinition(resolver_path="foo.bar.Baz")
        assert not d.is_per_side
        assert d.resolver_path == "foo.bar.Baz"

    def test_per_side_both(self):
        d = RemoteConfigDefinition(
            src_resolver_path="a.B", dst_resolver_path="c.D"
        )
        assert d.is_per_side

    def test_per_side_src_only(self):
        d = RemoteConfigDefinition(src_resolver_path="a.B")
        assert d.is_per_side

    def test_per_side_dst_only(self):
        d = RemoteConfigDefinition(dst_resolver_path="c.D")
        assert d.is_per_side

    def test_neither_raises(self):
        with pytest.raises(ValueError, match="Must specify"):
            RemoteConfigDefinition()

    def test_mixed_raises(self):
        with pytest.raises(ValueError, match="Cannot mix"):
            RemoteConfigDefinition(
                resolver_path="a.B", src_resolver_path="c.D"
            )

    def test_migration_fields(self):
        d = RemoteConfigDefinition(
            src_resolver_path="a.B",
            tables=["t1"],
            schema_name="myschema",
            exclude_users=["u1"],
        )
        assert d.tables == ["t1"]
        assert d.schema_name == "myschema"
        assert d.exclude_users == ["u1"]


class TestResolvePerSide:
    def _write_config(self, tmp_path, db, dc, config_dict):
        conf_dir = tmp_path / "remote-configs" / dc / db
        conf_dir.mkdir(parents=True, exist_ok=True)
        (conf_dir / "config.json").write_text(json.dumps(config_dict))

    def test_both_sides(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeSrcResolver",
            "src_resolver_config": {"app": "myapp", "pgrs_path": "/pgrs"},
            "dst_resolver_path": f"{__name__}.FakeDstResolver",
            "dst_resolver_config": {"secret_arn": "arn:dst"},
            "tables": ["users"],
            "schema_name": "public",
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is not None
        assert config.src.host == "src.example.com"
        assert config.dst.host == "dst.example.com"
        assert config.src.pglogical_user.pw == "srcpglpw"
        assert config.dst.pglogical_user.pw == "dstpglpw"
        assert config.tables == ["users"]
        assert config.schema_name == "public"

    def test_skip_src(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeSrcResolver",
            "src_resolver_config": {"app": "myapp", "pgrs_path": "/pgrs"},
            "dst_resolver_path": f"{__name__}.FakeDstResolver",
            "dst_resolver_config": {"secret_arn": "arn:dst"},
        })

        config = asyncio.run(
            resolve_remote_config("mydb", "mydc", skip_src=True)
        )
        assert config is not None
        assert config.src is None
        assert config.dst is not None

    def test_skip_dst(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeSrcResolver",
            "src_resolver_config": {"app": "myapp", "pgrs_path": "/pgrs"},
            "dst_resolver_path": f"{__name__}.FakeDstResolver",
            "dst_resolver_config": {"secret_arn": "arn:dst"},
        })

        config = asyncio.run(
            resolve_remote_config("mydb", "mydc", skip_dst=True)
        )
        assert config is not None
        assert config.src is not None
        assert config.dst is None

    def test_src_only_resolver(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeSrcResolver",
            "src_resolver_config": {"app": "myapp", "pgrs_path": "/pgrs"},
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is not None
        assert config.src is not None
        assert config.dst is None

    def test_resolver_returns_none_fails(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeNoneResolver",
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is None

    def test_resolver_error_fails(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeErrorResolver",
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is None

    def test_bad_module_fails(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": "nonexistent.module.Cls",
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is None

    def test_not_side_resolver_fails(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeLegacyResolver",
            "src_resolver_config": {"src_app": "x"},
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is None

    def test_migration_fields_pass_through(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "src_resolver_path": f"{__name__}.FakeSrcResolver",
            "src_resolver_config": {"app": "a", "pgrs_path": "/p"},
            "dst_resolver_path": f"{__name__}.FakeDstResolver",
            "dst_resolver_config": {"secret_arn": "arn:x"},
            "tables": ["t1", "t2"],
            "sequences": ["s1"],
            "schema_name": "myschema",
            "exclude_users": ["dd"],
            "exclude_patterns": ["%rep%"],
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config.tables == ["t1", "t2"]
        assert config.sequences == ["s1"]
        assert config.schema_name == "myschema"
        assert config.exclude_users == ["dd"]
        assert config.exclude_patterns == ["%rep%"]


class TestLegacyBackwardCompat:
    def _write_config(self, tmp_path, db, dc, config_dict):
        conf_dir = tmp_path / "remote-configs" / dc / db
        conf_dir.mkdir(parents=True, exist_ok=True)
        (conf_dir / "config.json").write_text(json.dumps(config_dict))

    def test_legacy_resolver_still_works(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_config(tmp_path, "mydb", "mydc", {
            "resolver_path": f"{__name__}.FakeLegacyResolver",
            "src_app": "myapp",
        })

        config = asyncio.run(resolve_remote_config("mydb", "mydc"))
        assert config is not None
        assert config.src.host == "src.example.com"
        assert config.dst.host == "dst.example.com"

    def test_no_config_file_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = asyncio.run(resolve_remote_config("nope", "nodc"))
        assert config is None
