# Remote Configuration & Resolvers

When pgbelt runs as a hosted service (rather than interactively on a developer
laptop) database credentials and migration settings are not stored in local
`config.json` files. Instead, a **remote-config JSON** tells pgbelt *how* to
fetch that information at runtime, and **resolvers** do the actual fetching.

## How it works

1. An orchestrator (e.g. Step Functions) launches a pgbelt command inside a
   container and writes a remote-config JSON to
   `remote-configs/{dc}/{db}/config.json`.
2. `resolve_remote_config(db, dc)` reads that file, dynamically imports the
   resolver class(es) it references, calls `resolve()`, and returns a
   `DbupgradeConfig` that the rest of pgbelt uses.

## Remote-config JSON schema

The JSON supports two mutually exclusive modes: **legacy** (single resolver)
and **per-side** (independent resolvers for source and destination).

### Legacy mode

A single `BaseResolver` subclass is responsible for returning a complete
`DbupgradeConfig` (both source and destination).

```json
{
  "resolver_path": "mypackage.resolvers.MyResolver",
  "app": "my-database",
  "tables": [],
  "sequences": []
}
```

All keys other than `resolver_path` are passed to the resolver constructor as
extra fields.

### Per-side mode

Each side of the migration has its own `BaseSideResolver` subclass that returns
a `DbConfig` for just that side. Resolver-specific configuration is scoped
under `src_resolver_config` / `dst_resolver_config` to avoid key collisions
when different resolver types are used.

```json
{
  "src_resolver_path": "dacloud_dbconfig.resolver.DacloudSideResolver",
  "dst_resolver_path": "mypackage.resolvers.PushedSecretSideResolver",
  "src_resolver_config": {
    "app": "my-database",
    "pgrs_path": "/infra/pgrs/..."
  },
  "dst_resolver_config": {
    "secret_name": "pgbaas/prod/my-database/dst"
  },
  "tables": [],
  "sequences": [],
  "schema_name": "public"
}
```

You can omit either `src_resolver_path` or `dst_resolver_path` if you only
need one side resolved (e.g. when running a command with `skip_src` or
`skip_dst`).

### Migration-level fields

These fields live at the top level of the JSON and are handled by the
framework, not by individual resolvers:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tables` | `list[str]` | `null` | Subset of tables to migrate |
| `sequences` | `list[str]` | `null` | Subset of sequences to sync |
| `schema_name` | `str` | `"public"` | Schema to operate on |
| `exclude_users` | `list[str]` | `null` | Users to exclude from login revocation |
| `exclude_patterns` | `list[str]` | `null` | LIKE patterns to exclude from login revocation |

## Writing a resolver

### BaseSideResolver (recommended)

Subclass `BaseSideResolver` to build a resolver that handles one side of a
migration. This is the preferred contract for new resolvers.

```python
from pgbelt.config.remote import BaseSideResolver, RemoteConfigError
from pgbelt.config.models import DbConfig

class MySideResolver(BaseSideResolver):
    # Extra fields populated from src_resolver_config or dst_resolver_config
    secret_name: str

    async def resolve(self) -> DbConfig | None:
        # self.db   -- the database pair name
        # self.dc   -- the datacenter / project name
        # self.logger -- pre-configured logger

        creds = await fetch_creds(self.secret_name)
        if creds is None:
            return None

        return DbConfig(
            host=creds["host"],
            ip=creds["ip"],
            db=creds["dbname"],
            port=creds["port"],
            root_user={"name": creds["root_user"], "pw": creds["root_pw"]},
            owner_user={"name": creds["owner_user"], "pw": creds["owner_pw"]},
            pglogical_user={"name": "pglogical", "pw": creds["pgl_pw"]},
        )
```

Key points:

- **Return a `DbConfig`**, not a `DbupgradeConfig`. The framework assembles
  the full config from the two sides plus the migration-level fields.
- **Manage your own pglogical password.** Each side resolver generates or
  retrieves an independent `pglogical_user.pw`. The framework passes it
  through without modification.
- **Raise `RemoteConfigError`** on retrieval failures, or return `None` if no
  config exists and that is not an error.
- **Extra fields** come from the `src_resolver_config` or `dst_resolver_config`
  dict in the remote-config JSON.

### BaseResolver (legacy)

The original contract. A single resolver is responsible for both sides:

```python
from pgbelt.config.remote import BaseResolver, RemoteConfigError
from pgbelt.config.models import DbupgradeConfig

class MyResolver(BaseResolver):
    # Extra fields populated from the top-level remote-config JSON
    app: str

    async def resolve(self) -> DbupgradeConfig | None:
        # self.db, self.dc, self.skip_src, self.skip_dst, self.logger
        ...
        return DbupgradeConfig(...)
```

This contract is fully supported for backward compatibility. New resolvers
should prefer `BaseSideResolver`.

## Why per-side resolvers?

The legacy contract requires a single resolver class to fetch credentials for
*both* source and destination databases. When the two sides use different
credential backends (e.g. Dacloud PGRS for source, AWS Secrets Manager for
destination), you'd need a composite resolver that wraps two different
libraries -- duplicating logic and creating coupling.

Per-side resolvers let each side be an independent, focused class:

- **No composites needed.** Mix and match resolvers freely in the remote-config
  JSON.
- **Independent pglogical passwords.** The legacy contract shared a single
  pglogical password across both sides (a bug). Per-side resolvers each manage
  their own.
- **Simpler to write and test.** Each resolver handles exactly one concern.
