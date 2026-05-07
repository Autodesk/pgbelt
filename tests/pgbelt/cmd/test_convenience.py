import logging
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import asyncpg
import pytest
from pgbelt.cmd import convenience


# Test for the Owner DSN to come from src-dsn when the config
# has an Owner in it
def test_src_dsn_owner(config):
    with patch("pgbelt.cmd.convenience.get_config", return_value=config):
        convenience.echo = Mock()
        convenience.src_dsn("test-db", "test-dc")
        convenience.echo.assert_called_with(config.src.owner_dsn)


# ---------------------------------------------------------------------------
# Owner-credential probe -- the regression target for
# https://jira.autodesk.com/browse/STOPS-7717
# ---------------------------------------------------------------------------
#
# ``belt precheck`` opens an owner-pool against the source via
# ``asyncpg.create_pool(conf.src.owner_uri, min_size=1)``. With a wrong
# or stale owner password against a managed RDS (which silently closes
# the connection rather than returning a structured auth error), this
# hangs for the full ``connect_timeout=60s`` and surfaces a bare
# ``TimeoutError`` with no message. ``check-connectivity`` now probes
# owner-creds upfront with a 10s timeout, so the failure becomes a
# clean ``src_owner_query=false`` cell on the per-DB grid.


@pytest.fixture
def logger():
    return logging.getLogger("test.owner-probe")


class TestCheckOwnerQuery:
    @pytest.mark.asyncio
    async def test_returns_true_on_successful_select(self, logger):
        """Happy path: connect + SELECT 1 + close == probe passes."""
        conn = AsyncMock()
        conn.fetchval.return_value = 1
        with patch(
            "pgbelt.cmd.convenience.asyncpg.connect",
            new=AsyncMock(return_value=conn),
        ) as mock_connect:
            ok = await convenience._check_owner_query(
                "postgresql://owner:p@h:5432/db", "src owner", logger, timeout=5.0
            )
        assert ok is True
        mock_connect.assert_awaited_once_with(
            "postgresql://owner:p@h:5432/db", timeout=5.0
        )
        conn.fetchval.assert_awaited_once_with("SELECT 1")
        conn.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_false_on_connect_timeout(self, logger):
        """asyncpg's own ``timeout`` triggers ``TimeoutError`` -- caught
        explicitly so we can log the actionable hint and avoid the 60s
        default that motivated this whole probe."""
        with patch(
            "pgbelt.cmd.convenience.asyncpg.connect",
            new=AsyncMock(side_effect=TimeoutError()),
        ):
            ok = await convenience._check_owner_query(
                "postgresql://owner:bad@h:5432/db",
                "src owner",
                logger,
                timeout=0.5,
            )
        assert ok is False

    @pytest.mark.asyncio
    async def test_returns_false_on_invalid_password(self, logger):
        """A structured auth error is the cleaner of the two failure
        modes (vs the silent timeout path); make sure it's also caught
        rather than bubbling up."""
        with patch(
            "pgbelt.cmd.convenience.asyncpg.connect",
            new=AsyncMock(
                side_effect=asyncpg.exceptions.InvalidPasswordError(
                    "password authentication failed for user 'owner'"
                )
            ),
        ):
            ok = await convenience._check_owner_query(
                "postgresql://owner:wrong@h:5432/db",
                "src owner",
                logger,
            )
        assert ok is False

    @pytest.mark.asyncio
    async def test_returns_false_when_select_fails_but_still_closes(self, logger):
        """Auth succeeded but the server killed the session before
        SELECT 1 returned. Still must return False AND release the
        connection so we don't leak it back into the pool tests."""
        conn = AsyncMock()
        conn.fetchval.side_effect = RuntimeError("server closed the connection")
        with patch(
            "pgbelt.cmd.convenience.asyncpg.connect",
            new=AsyncMock(return_value=conn),
        ):
            ok = await convenience._check_owner_query(
                "postgresql://owner:p@h:5432/db", "src owner", logger
            )
        assert ok is False
        conn.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_failure_is_swallowed(self, logger):
        """``conn.close()`` raising must not mask a successful probe --
        the probe's whole job is to answer 'can owner authenticate?'
        and a stuck close is unrelated noise."""
        conn = AsyncMock()
        conn.fetchval.return_value = 1
        conn.close.side_effect = RuntimeError("close timed out")
        with patch(
            "pgbelt.cmd.convenience.asyncpg.connect",
            new=AsyncMock(return_value=conn),
        ):
            ok = await convenience._check_owner_query(
                "postgresql://owner:p@h:5432/db", "src owner", logger
            )
        assert ok is True


class TestCheckConnectivityOwnerProbe:
    """End-to-end check: ``check_connectivity`` must run the owner probe
    once root SELECT 1 succeeds, and surface its boolean verdict in the
    returned dict so ``ConnectivityCheckRow`` can serialize the new
    ``{src,dst}_owner_query`` fields."""

    @pytest.mark.asyncio
    async def test_owner_probe_invoked_when_root_query_passes(self, config):
        """Both root SELECT 1s pass -> both owner probes get called
        with the matching side's ``owner_uri``."""

        async def _config_future():
            return config

        root_pool = AsyncMock()
        root_pool.fetchval.return_value = 1
        root_pool.close = AsyncMock()

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp", new=AsyncMock(return_value=True)
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=AsyncMock(return_value=root_pool),
            ),
            patch("pgbelt.cmd.convenience.ensure_dblink", new=AsyncMock()),
            patch(
                "pgbelt.cmd.convenience.check_connectivity_via_dblink",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=True),
            ) as mock_owner,
        ):
            result = await convenience.check_connectivity.__wrapped__(_config_future())

        assert result["src_owner_query"] is True
        assert result["dst_owner_query"] is True
        assert mock_owner.await_count == 2
        called_uris = {c.args[0] for c in mock_owner.await_args_list}
        assert config.src.owner_uri in called_uris
        assert config.dst.owner_uri in called_uris

    @pytest.mark.asyncio
    async def test_owner_probe_skipped_when_root_query_fails(self, config):
        """If root SELECT 1 fails (network / RDS-level issue), the
        owner probe is skipped on that side -- the underlying problem
        is already surfaced via ``*_query`` and we don't want to
        double-flag it as a credential issue."""

        async def _config_future():
            return config

        root_pool = AsyncMock()
        root_pool.fetchval.side_effect = RuntimeError("connection refused")
        root_pool.close = AsyncMock()

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp", new=AsyncMock(return_value=True)
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=AsyncMock(return_value=root_pool),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=True),
            ) as mock_owner,
        ):
            result = await convenience.check_connectivity.__wrapped__(_config_future())

        assert result["src_query"] is False
        assert result["dst_query"] is False
        assert result["src_owner_query"] is False
        assert result["dst_owner_query"] is False
        mock_owner.assert_not_awaited()
