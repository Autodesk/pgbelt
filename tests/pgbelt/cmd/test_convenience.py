from pgbelt.cmd import convenience
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch


# Test for the Owner DSN to come from src-dsn when the config
# has an Owner in it
def test_src_dsn_owner(config):
    with patch("pgbelt.cmd.convenience.get_config", return_value=config):
        convenience.echo = Mock()
        convenience.src_dsn("test-db", "test-dc")
        convenience.echo.assert_called_with(config.src.owner_dsn)
