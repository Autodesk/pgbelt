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
        assert ok == (True, None)
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
        assert ok[0] is False
        assert "timed out after 0.5s" in ok[1]

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
        assert ok[0] is False
        assert "InvalidPasswordError" in ok[1]
        assert "password authentication failed" in ok[1]

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
        assert ok[0] is False
        assert "server closed the connection" in ok[1]
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
        assert ok == (True, None)


def _root_pool(fetchval_side_effect=None):
    pool = AsyncMock()
    pool.fetchval.return_value = 1
    if fetchval_side_effect is not None:
        pool.fetchval.side_effect = fetchval_side_effect
    pool.close = AsyncMock()
    return pool


def _pool_factory(config, src=None, dst=None):
    """Return a ``create_pool`` stand-in that hands out (or raises) per side,
    keyed on the root URI it's called with."""

    async def _create_pool(uri, **kwargs):
        outcome = src if uri == config.src.root_uri else dst
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return _create_pool


async def _run_check_connectivity(config):
    async def _config_future():
        return config

    return await convenience.check_connectivity.__wrapped__(_config_future())


class TestCheckConnectivityOwnerProbe:
    """End-to-end check: ``check_connectivity`` must run the owner probe
    once root SELECT 1 succeeds, and surface its boolean verdict in the
    returned dict so ``ConnectivityCheckRow`` can serialize the
    ``{src,dst}_owner_query`` fields."""

    @pytest.mark.asyncio
    async def test_owner_probe_invoked_when_root_query_passes(self, config):
        """Both root SELECT 1s pass -> both owner probes get called
        with the matching side's ``owner_uri``."""
        src_pool, dst_pool = _root_pool(), _root_pool()

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp",
                new=AsyncMock(return_value=(True, None)),
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=_pool_factory(config, src=src_pool, dst=dst_pool),
            ),
            patch("pgbelt.cmd.convenience.ensure_dblink", new=AsyncMock()),
            patch(
                "pgbelt.cmd.convenience.check_connectivity_via_dblink",
                new=AsyncMock(return_value=(True, None)),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=(True, None)),
            ) as mock_owner,
        ):
            result = await _run_check_connectivity(config)

        assert result["src_owner_query"] is True
        assert result["dst_owner_query"] is True
        assert result["errors"] == {}
        assert mock_owner.await_count == 2
        called_uris = {c.args[0] for c in mock_owner.await_args_list}
        assert config.src.owner_uri in called_uris
        assert config.dst.owner_uri in called_uris
        src_pool.close.assert_awaited_once()
        dst_pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_owner_probe_skipped_when_root_query_fails(self, config):
        """If root SELECT 1 fails (network / RDS-level issue), the
        owner probe is skipped on that side -- the underlying problem
        is already surfaced via ``*_query`` and we don't want to
        double-flag it as a credential issue."""
        src_pool = _root_pool(RuntimeError("connection refused"))
        dst_pool = _root_pool(RuntimeError("connection refused"))

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp",
                new=AsyncMock(return_value=(True, None)),
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=_pool_factory(config, src=src_pool, dst=dst_pool),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=(True, None)),
            ) as mock_owner,
        ):
            result = await _run_check_connectivity(config)

        assert result["src_query"] is False
        assert result["dst_query"] is False
        assert result["src_owner_query"] is False
        assert result["dst_owner_query"] is False
        assert "connection refused" in result["errors"]["src_query"]
        assert result["errors"]["src_owner_query"] == "skipped: src_query failed"
        mock_owner.assert_not_awaited()


class TestCheckConnectivityPerSideFailures:
    """STOPS-8335: an auth failure while creating one side's root pool used
    to raise out of ``gather`` and abort the whole command, so the JSON
    output had a generic error with no side. Each side is now probed on its
    own and the error text is reported per check."""

    @pytest.mark.asyncio
    async def test_dst_pg_hba_rejection_names_the_side(self, config):
        src_pool = _root_pool()
        pg_hba = asyncpg.exceptions.InvalidAuthorizationSpecificationError(
            'no pg_hba.conf entry for host "10.91.212.82", user "postgres", '
            'database "assets_db", no encryption'
        )

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp",
                new=AsyncMock(return_value=(True, None)),
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=_pool_factory(config, src=src_pool, dst=pg_hba),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=(True, None)),
            ) as mock_owner,
            patch(
                "pgbelt.cmd.convenience.check_connectivity_via_dblink",
                new=AsyncMock(return_value=(True, None)),
            ) as mock_dblink,
        ):
            result = await _run_check_connectivity(config)

        # The source side still ran all of its own probes.
        assert result["src_tcp"] is True
        assert result["src_query"] is True
        assert result["src_owner_query"] is True
        mock_owner.assert_awaited_once()
        assert mock_owner.await_args.args[0] == config.src.owner_uri

        # The destination failure names the side, the host and the cause.
        assert result["dst_query"] is False
        assert result["dst_host"] == config.dst.host
        assert result["src_host"] == config.src.host
        dst_error = result["errors"]["dst_query"]
        assert "InvalidAuthorizationSpecificationError" in dst_error
        assert "no pg_hba.conf entry" in dst_error
        assert result["errors"]["dst_owner_query"] == "skipped: dst_query failed"
        assert result["errors"]["src_to_dst_dblink"] == "skipped: dst_query failed"
        assert "src_query" not in result["errors"]

        # dblink needs both sides, so it's skipped; the src pool is closed.
        mock_dblink.assert_not_awaited()
        src_pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_src_tcp_failure_does_not_block_dst(self, config):
        dst_pool = _root_pool()

        async def _tcp(host, port, logger):
            if host == config.src.ip:
                return False, f"timed out connecting to {host}:{port} after 3s"
            return True, None

        with (
            patch("pgbelt.cmd.convenience._check_tcp", new=_tcp),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=_pool_factory(config, src=None, dst=dst_pool),
            ),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=(False, "owner SELECT 1 failed: boom")),
            ),
        ):
            result = await _run_check_connectivity(config)

        assert result["src_tcp"] is False
        assert "timed out connecting to 192.168.0.11" in result["errors"]["src_tcp"]
        assert result["errors"]["src_query"] == "skipped: src_tcp failed"
        assert result["dst_query"] is True
        assert result["errors"]["dst_owner_query"] == "owner SELECT 1 failed: boom"
        dst_pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dblink_failure_reports_remote_error(self, config):
        src_pool, dst_pool = _root_pool(), _root_pool()

        async def _dblink(pool, remote_dsn, logger):
            if pool is src_pool:
                return False, "dblink connection failed: boom (no pg_hba.conf entry)"
            return True, None

        with (
            patch(
                "pgbelt.cmd.convenience._check_tcp",
                new=AsyncMock(return_value=(True, None)),
            ),
            patch(
                "pgbelt.cmd.convenience.create_pool",
                new=_pool_factory(config, src=src_pool, dst=dst_pool),
            ),
            patch("pgbelt.cmd.convenience.ensure_dblink", new=AsyncMock()),
            patch("pgbelt.cmd.convenience.check_connectivity_via_dblink", new=_dblink),
            patch(
                "pgbelt.cmd.convenience._check_owner_query",
                new=AsyncMock(return_value=(True, None)),
            ),
        ):
            result = await _run_check_connectivity(config)

        assert result["src_to_dst_dblink"] is False
        assert result["dst_to_src_dblink"] is True
        assert result["errors"] == {
            "src_to_dst_dblink": "dblink connection failed: boom (no pg_hba.conf entry)"
        }


class TestCheckTcp:
    @pytest.mark.asyncio
    async def test_refused_returns_error_text(self, logger):
        with patch(
            "pgbelt.cmd.convenience.open_connection",
            new=AsyncMock(side_effect=ConnectionRefusedError(61, "Connection refused")),
        ):
            ok, error = await convenience._check_tcp("10.0.0.1", "5432", logger)
        assert ok is False
        assert "ConnectionRefusedError" in error
        assert "10.0.0.1:5432" in error

    @pytest.mark.asyncio
    async def test_unreachable_host_is_caught(self, logger):
        """OSError subclasses other than refused/gaierror (e.g. no route to
        host) used to escape and abort the command."""
        with patch(
            "pgbelt.cmd.convenience.open_connection",
            new=AsyncMock(side_effect=OSError(65, "No route to host")),
        ):
            ok, error = await convenience._check_tcp("10.0.0.1", "5432", logger)
        assert ok is False
        assert "No route to host" in error


class TestPrintConnectivityResults:
    @pytest.mark.asyncio
    async def test_lists_failed_checks_with_side_and_host(self, monkeypatch):
        echo = Mock()
        monkeypatch.setattr(convenience, "echo", echo)
        row = {
            "db": "assets_db",
            "src_tcp": True,
            "src_query": True,
            "src_owner_query": True,
            "src_to_dst_dblink": False,
            "dst_tcp": True,
            "dst_query": False,
            "dst_owner_query": False,
            "dst_to_src_dblink": False,
            "src_host": "src.example",
            "dst_host": "dst.example",
            "errors": {
                "dst_query": "root connection failed: no pg_hba.conf entry",
                "dst_owner_query": "skipped: dst_query failed",
                "src_to_dst_dblink": "skipped: dst_query failed",
                "dst_to_src_dblink": "skipped: dst_query failed",
            },
        }
        with pytest.raises(SystemExit):
            await convenience._print_connectivity_results([row])
        out = echo.call_args[0][0]
        assert (
            "assets_db: dst_query (dst host dst.example): "
            "root connection failed: no pg_hba.conf entry"
        ) in out
