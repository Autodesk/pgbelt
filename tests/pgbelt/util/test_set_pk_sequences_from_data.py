"""Unit tests for the PK ``setval`` SQL emitted by ``sync-sequences``.

The async ``set_pk_sequences_from_data`` defers all SQL rendering to the
pure helper ``_build_pk_setval_sql``; testing that helper directly avoids
needing an event loop or asyncpg pool fixtures.

Locks in the post-STOPS-7796 behavior: each PK sequence is bumped to
``GREATEST(coalesce(max(<pk_col>), 1), <src.last_value>)`` so a healthy
cutover with caught-up replication passes ``diff-sequences`` even when
the source sequence is ahead of ``max(pk_col)`` (rolled-back inserts,
``ON CONFLICT DO NOTHING`` losers, deletes of the highest id, ``nextval()``
caches).
"""

from pgbelt.util.postgres import _build_pk_setval_sql


def test_empty_pk_seqs_returns_empty_string():
    assert _build_pk_setval_sql({}, "public") == ""


def test_uses_greatest_of_pk_max_and_src_last_value():
    sql = _build_pk_setval_sql(
        {"users_id_seq": ("users", "id")},
        "public",
        src_pk_vals={"users_id_seq": 30412},
    )
    assert "setval('public.\"users_id_seq\"'" in sql
    assert 'GREATEST(coalesce(max("id"), 1), 30412)' in sql
    # is_called: true when DST has rows OR src has been called past 1.
    assert '(max("id") IS NOT null) OR (30412 > 1)' in sql
    assert 'FROM public."users"' in sql


def test_falls_back_to_dst_max_when_src_pk_vals_omitted():
    # Backwards compatibility: src_last renders as 0 so GREATEST
    # collapses to coalesce(max(col), 1) and the is_called flag is
    # keyed purely off max(col) IS NOT null -- same observable
    # behavior as the pre-fix code.
    sql = _build_pk_setval_sql(
        {"orders_id_seq": ("orders", "order_id")},
        "public",
    )
    assert 'GREATEST(coalesce(max("order_id"), 1), 0)' in sql
    assert "(0 > 1)" in sql


def test_missing_per_sequence_src_value_treated_as_zero():
    sql = _build_pk_setval_sql(
        {
            "a_id_seq": ("a", "id"),
            "b_id_seq": ("b", "id"),
        },
        "public",
        src_pk_vals={"a_id_seq": 42},  # b_id_seq deliberately absent
    )
    assert 'GREATEST(coalesce(max("id"), 1), 42)' in sql
    assert 'GREATEST(coalesce(max("id"), 1), 0)' in sql


def test_quotes_schema_table_and_column_names():
    # PascalCase / mixed-case identifiers must stay double-quoted on
    # both sides of setval so PG doesn't fold them to lowercase.
    sql = _build_pk_setval_sql(
        {"UsersCapital_id_seq": ("UsersCapital", "Id")},
        "PgbaasDvSrcSchema",
        src_pk_vals={"UsersCapital_id_seq": 7},
    )
    assert "setval('PgbaasDvSrcSchema.\"UsersCapital_id_seq\"'" in sql
    assert 'FROM PgbaasDvSrcSchema."UsersCapital"' in sql
    assert 'coalesce(max("Id"), 1)' in sql


def test_src_last_value_coerced_to_int():
    # ``int(...)`` defensively coerces in case the upstream value is
    # ever something other than int (e.g. a str). Guarantees the value
    # is never SQL-quoted as a literal string.
    sql = _build_pk_setval_sql(
        {"x_id_seq": ("x", "id")},
        "public",
        src_pk_vals={"x_id_seq": "99"},  # type: ignore[dict-item]
    )
    assert 'GREATEST(coalesce(max("id"), 1), 99)' in sql
    assert "'99'" not in sql


def test_multiple_sequences_concatenated_with_newlines():
    sql = _build_pk_setval_sql(
        {
            "a_id_seq": ("a", "id"),
            "b_id_seq": ("b", "id"),
        },
        "public",
        src_pk_vals={"a_id_seq": 5, "b_id_seq": 10},
    )
    statements = [s for s in sql.split("\n") if s.strip()]
    assert len(statements) == 2
    for stmt in statements:
        assert stmt.endswith(";")
        assert stmt.startswith("SELECT setval(")
