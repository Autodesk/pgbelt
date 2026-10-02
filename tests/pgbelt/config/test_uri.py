"""Connection URIs must preserve passwords containing URI delimiters."""

from urllib.parse import unquote
from urllib.parse import urlsplit

import pytest

from pgbelt.config.models import DbConfig
from pgbelt.config.models import User


@pytest.mark.parametrize("role", ["root", "owner", "pglogical"])
@pytest.mark.parametrize("password", ["plain", "one/two", "/leading", "a/@:#?%"])
def test_uri_password_round_trip(role, password):
    user = User(name="example", pw=password)
    config = DbConfig(
        host="db.example",
        ip="127.0.0.1",
        port="5432",
        db="example",
        root_user=user,
        owner_user=user,
        pglogical_user=user,
    )

    parsed = urlsplit(getattr(config, f"{role}_uri"))

    assert parsed.hostname == "127.0.0.1"
    assert parsed.port == 5432
    assert parsed.path == "/example"
    assert parsed.username == "example"
    assert unquote(parsed.password) == password
