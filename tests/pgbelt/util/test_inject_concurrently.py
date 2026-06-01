from pgbelt.util.dump import _inject_concurrently


def test_basic_create_index():
    out = _inject_concurrently("CREATE INDEX idx_foo ON public.t (a)")
    assert out == "CREATE INDEX CONCURRENTLY idx_foo ON public.t (a)"


def test_create_unique_index():
    out = _inject_concurrently("CREATE UNIQUE INDEX idx_foo ON public.t (a)")
    assert out == "CREATE UNIQUE INDEX CONCURRENTLY idx_foo ON public.t (a)"


def test_case_insensitive():
    out = _inject_concurrently("create index idx_foo on public.t (a)")
    assert out == "create index CONCURRENTLY idx_foo on public.t (a)"


def test_idempotent():
    once = _inject_concurrently("CREATE INDEX idx_foo ON public.t (a)")
    twice = _inject_concurrently(once)
    assert once == twice


def test_idempotent_with_existing_concurrently():
    src = "CREATE INDEX CONCURRENTLY idx_foo ON public.t (a)"
    assert _inject_concurrently(src) == src


def test_extra_whitespace_preserved():
    out = _inject_concurrently("CREATE  INDEX  idx_foo ON public.t (a)")
    assert out == "CREATE  INDEX CONCURRENTLY  idx_foo ON public.t (a)"


def test_hash_index_with_expression():
    # Mirrors the real-world projectevents14 case (STOPS-7797): a hash
    # index over a jsonb extraction expression.
    src = (
        "CREATE INDEX project_events_idx_field_report_type_uid ON "
        "public.project_events USING hash "
        "(((event_data ->> 'field_report_type_uid'::text)))"
    )
    out = _inject_concurrently(src)
    assert out.startswith("CREATE INDEX CONCURRENTLY ")
    # Ensure we didn't double-inject inside the expression body
    assert out.count("CONCURRENTLY") == 1


def test_leading_comment_and_set_block():
    # pg_dump output typically includes SET statements and comments before
    # the CREATE INDEX. The function operates on individual statements
    # (split on ;) but should still inject correctly if a single statement
    # has leading whitespace / SET.
    src = "\n  CREATE INDEX idx_foo ON public.t (a)"
    out = _inject_concurrently(src)
    assert out == "\n  CREATE INDEX CONCURRENTLY idx_foo ON public.t (a)"


def test_non_create_index_unchanged():
    src = "ALTER TABLE foo ADD CONSTRAINT bar CHECK (a > 0) NOT VALID"
    assert _inject_concurrently(src) == src


def test_empty_string_unchanged():
    assert _inject_concurrently("") == ""
