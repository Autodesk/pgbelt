import sys
import time
from asyncio import gather
from asyncio import run
from collections.abc import Awaitable
from collections.abc import Callable
from functools import wraps
from inspect import iscoroutinefunction
from inspect import Parameter
from inspect import signature
from typing import Any
from typing import Optional  # noqa: F401 # Needed until tiangolo/typer#522 is fixed)
from typing import TypeVar

from pgbelt.config import get_all_configs_async
from pgbelt.config import get_config_async
from pgbelt.models.base import CommandError
from pgbelt.models.base import CommandResult
from pgbelt.models.connectivity import ConnectivityCheckResult
from pgbelt.models.connectivity import ConnectivityCheckRow
from pgbelt.models.connections import ConnectionsResult
from pgbelt.models.connections import ConnectionsRow
from pgbelt.models.connections import ConnectionsSide
from pgbelt.models.preflight import ExtensionInfo
from pgbelt.models.preflight import PrecheckResult
from pgbelt.models.preflight import PrecheckSide
from pgbelt.models.preflight import RelationInfo
from pgbelt.models.preflight import RoleInfo
from pgbelt.models.preflight import TableReplicationInfo
from pgbelt.models.schema import CreateIndexesResult
from pgbelt.models.schema import DiffSchemaRow
from pgbelt.models.schema import DiffSchemasResult
from pgbelt.models.schema import IndexDetail
from pgbelt.models.status import ReplicationLag
from pgbelt.models.status import StatusResult
from pgbelt.models.status import StatusRow
from pgbelt.models.sync import SequenceSyncDetail
from pgbelt.models.sync import SyncSequencesResult
from pgbelt.models.sync import SyncTablesResult
from pgbelt.models.sync import TableSyncDetail
from pgbelt.models.sync import TableValidationDetail
from pgbelt.models.sync import ValidateDataResult
from typer import Argument
from typer import Option
from typer import Typer


T = TypeVar("T")


def _build_connectivity_result(
    results: list[dict], base_kwargs: dict
) -> ConnectivityCheckResult:
    rows = [ConnectivityCheckRow(**r) for r in results if isinstance(r, dict)]
    return ConnectivityCheckResult(
        success=all(r.all_ok for r in rows),
        results=rows,
        **base_kwargs,
    )


def _build_connections_result(
    results: list[dict], base_kwargs: dict
) -> ConnectionsResult:
    rows = []
    for r in results:
        if not isinstance(r, dict):
            continue
        rows.append(
            ConnectionsRow(
                db=r["db"],
                source=ConnectionsSide(
                    total_connections=r["src_count"],
                    by_user=r.get("src_usernames", {}),
                ),
                destination=ConnectionsSide(
                    total_connections=r["dst_count"],
                    by_user=r.get("dst_usernames", {}),
                ),
            )
        )
    return ConnectionsResult(success=True, results=rows, **base_kwargs)


def _build_status_result(results: list[dict], base_kwargs: dict) -> StatusResult:
    rows = []
    for r in results:
        if not isinstance(r, dict):
            continue
        lag = ReplicationLag(
            sent_lag=r.get("sent_lag", "unknown"),
            write_lag=r.get("write_lag", "unknown"),
            flush_lag=r.get("flush_lag", "unknown"),
            replay_lag=r.get("replay_lag", "unknown"),
        )
        rows.append(
            StatusRow(
                db=r.get("db", ""),
                forward_replication=r.get("pg1_pg2", "unconfigured"),
                back_replication=r.get("pg2_pg1", "unconfigured"),
                lag=lag,
                src_dataset_size=r.get("src_dataset_size"),
                dst_dataset_size=r.get("dst_dataset_size"),
                progress=r.get("progress"),
            )
        )
    return StatusResult(success=True, results=rows, **base_kwargs)


def _as_list(val) -> list:
    """Coerce a value to a list. Postgres GUC strings come back as
    comma-separated text, not arrays."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    return list(val)


def _build_precheck_side(raw: dict, pkeys: list | None = None) -> PrecheckSide:
    """Convert the raw dict from precheck_info into a PrecheckSide model."""
    users_raw = raw.get("users", {})
    root_raw = users_raw.get("root", {})
    owner_raw = users_raw.get("owner", {})

    tables_raw = raw.get("tables", [])
    pkey_names = set(pkeys) if pkeys else set()

    tables = []
    for t in tables_raw:
        name = t.get("Name", "")
        schema = t.get("Schema", "")
        owner = t.get("Owner", "")
        has_pk = name in pkey_names
        can_rep = schema == raw.get("schema", "") and owner == owner_raw.get(
            "rolname", ""
        )
        if can_rep:
            method = "pglogical" if has_pk else "dump_and_load"
        else:
            method = "unavailable"
        tables.append(
            TableReplicationInfo(
                name=name,
                schema_name=schema,
                owner=owner,
                has_primary_key=has_pk,
                replication_method=method,
            )
        )

    sequences = [
        RelationInfo(
            name=s.get("Name", ""),
            schema_name=s.get("Schema", ""),
            owner=s.get("Owner", ""),
            object_type="sequence",
        )
        for s in raw.get("sequences", [])
    ]

    extensions = [
        ExtensionInfo(extname=e["extname"] if not isinstance(e, str) else e)
        for e in raw.get("extensions", [])
    ]

    return PrecheckSide(
        db=raw.get("db", ""),
        schema_name=raw.get("schema", "public"),
        server_version=raw.get("server_version", "unknown"),
        max_replication_slots=raw.get("max_replication_slots", "0"),
        max_worker_processes=raw.get("max_worker_processes", "0"),
        max_wal_senders=raw.get("max_wal_senders", "0"),
        shared_preload_libraries=_as_list(raw.get("shared_preload_libraries", [])),
        rds_logical_replication=raw.get("rds.logical_replication", "unknown"),
        root_user=RoleInfo(
            rolname=root_raw.get("rolname", ""),
            rolcanlogin=root_raw.get("rolcanlogin", False),
            rolcreaterole=root_raw.get("rolcreaterole", False),
            rolinherit=root_raw.get("rolinherit", False),
            rolsuper=root_raw.get("rolsuper", False),
            memberof=root_raw.get("memberof", []),
        ),
        owner_user=RoleInfo(
            rolname=owner_raw.get("rolname", ""),
            rolcanlogin=owner_raw.get("rolcanlogin", False),
            rolcreaterole=owner_raw.get("rolcreaterole", False),
            rolinherit=owner_raw.get("rolinherit", False),
            rolsuper=owner_raw.get("rolsuper", False),
            memberof=owner_raw.get("memberof", []),
            can_create=owner_raw.get("can_create"),
        ),
        tables=tables,
        sequences=sequences,
        extensions=extensions,
    )


def _build_precheck_result(results: list[dict], base_kwargs: dict) -> PrecheckResult:
    if len(results) == 1 and isinstance(results[0], dict):
        raw = results[0]
        src_raw = raw.get("src", {})
        dst_raw = raw.get("dst", {})
        src_side = _build_precheck_side(src_raw, pkeys=src_raw.get("pkeys"))
        dst_side = _build_precheck_side(dst_raw)

        src_ext_names = {e.extname for e in src_side.extensions}
        for ext in dst_side.extensions:
            ext.in_other_side = ext.extname in src_ext_names
        dst_ext_names = {e.extname for e in dst_side.extensions}
        for ext in src_side.extensions:
            ext.in_other_side = ext.extname in dst_ext_names

        return PrecheckResult(success=True, src=src_side, dst=dst_side, **base_kwargs)

    # Multi-DB: store raw dicts since we get separate src/dst per DB
    return PrecheckResult(success=True, **base_kwargs)


def _build_sync_sequences_result(
    results: list[dict], base_kwargs: dict
) -> SyncSequencesResult:
    if len(results) == 1 and isinstance(results[0], dict):
        r = results[0]
        return SyncSequencesResult(
            success=True,
            schema_name=r.get("schema_name"),
            stride=r.get("stride"),
            pk_sequences=[SequenceSyncDetail(**s) for s in r.get("pk_sequences", [])],
            non_pk_sequences=[
                SequenceSyncDetail(**s) for s in r.get("non_pk_sequences", [])
            ],
            **base_kwargs,
        )
    return SyncSequencesResult(success=True, **base_kwargs)


def _build_sync_tables_result(
    results: list[dict], base_kwargs: dict
) -> SyncTablesResult:
    if len(results) == 1 and isinstance(results[0], dict):
        r = results[0]
        return SyncTablesResult(
            success=True,
            schema_name=r.get("schema_name"),
            discovery_mode=r.get("discovery_mode", "auto"),
            tables=[TableSyncDetail(**t) for t in r.get("tables", [])],
            **base_kwargs,
        )
    return SyncTablesResult(success=True, **base_kwargs)


def _build_validate_data_result(
    results: list[dict], base_kwargs: dict
) -> ValidateDataResult:
    if len(results) == 1 and isinstance(results[0], dict):
        r = results[0]
        return ValidateDataResult(
            success=all(t.get("passed", True) for t in r.get("tables", [])),
            schema_name=r.get("schema_name"),
            tables=[TableValidationDetail(**t) for t in r.get("tables", [])],
            **base_kwargs,
        )
    return ValidateDataResult(success=True, **base_kwargs)


def _build_create_indexes_result(
    results: list[dict], base_kwargs: dict
) -> CreateIndexesResult:
    if len(results) == 1 and isinstance(results[0], dict):
        r = results[0]
        indexes = [IndexDetail(**i) for i in r.get("indexes", [])]
        has_failures = any(i.status == "failed" for i in indexes)
        return CreateIndexesResult(
            success=not has_failures,
            indexes_file=r.get("indexes_file"),
            indexes=indexes,
            analyze_ran=r.get("analyze_ran", False),
            **base_kwargs,
        )
    return CreateIndexesResult(success=True, **base_kwargs)


def _build_diff_schemas_result(
    results: list[dict], base_kwargs: dict
) -> DiffSchemasResult:
    rows = [
        DiffSchemaRow(
            db=r.get("db", ""),
            result=r.get("result", "skipped"),
            diff=r.get("diff"),
        )
        for r in results
        if isinstance(r, dict)
    ]
    has_mismatch = any(r.result == "mismatch" for r in rows)
    return DiffSchemasResult(
        success=not has_mismatch,
        results=rows,
        **base_kwargs,
    )


_RICH_MODEL_BUILDERS: dict[str, Callable] = {
    "check-connectivity": _build_connectivity_result,
    "connections": _build_connections_result,
    "status": _build_status_result,
    "precheck": _build_precheck_result,
    "sync-sequences": _build_sync_sequences_result,
    "sync-tables": _build_sync_tables_result,
    "validate-data": _build_validate_data_result,
    "create-indexes": _build_create_indexes_result,
    "diff-schemas": _build_diff_schemas_result,
}


def _build_json_output(
    command_name: str,
    dc: str,
    db: str,
    results: list,
    success: bool,
    duration_ms: int,
    error: Exception | None = None,
) -> str:
    """Build a CommandResult JSON string from raw command results."""
    cmd_error = None
    if error is not None:
        cmd_error = CommandError(
            error_type=type(error).__name__,
            message=str(error),
        )

    base_kwargs = dict(
        db=db or dc,
        dc=dc,
        duration_ms=duration_ms,
        error=cmd_error,
    )

    builder = _RICH_MODEL_BUILDERS.get(command_name)
    if builder and not cmd_error:
        result = builder(results, base_kwargs)
    else:
        detail = {}
        if results and len(results) == 1 and isinstance(results[0], dict):
            detail = results[0]
        elif results and all(isinstance(r, dict) for r in results):
            detail = {"databases": results}
        result = CommandResult(
            command=command_name,
            success=success,
            detail=detail,
            **base_kwargs,
        )

    return result.model_dump_json(indent=2)


def run_with_configs(
    decorated_func: Callable[..., Awaitable[Optional[T]]] = None,
    skip_src: bool = False,
    skip_dst: bool = False,
    results_callback: Optional[Callable[[list[T]], Awaitable[Optional[Any]]]] = None,
) -> Callable:
    """
    Decorator for async commands. Implementations should take one Awaitable[DbupgradeConfig] arg
    and do some operation on the databases in it. This wrapper handles looking up the
    config and executing the command. The decorated result can be run either on one db only
    or on the entire datacenter concurrently.

    You may also provide a callback that will be called on the results of the command. Useful
    for displaying the output of interrogative commands.

    When the caller passes json_mode=True the results_callback is skipped and structured
    JSON is printed to stdout instead.
    """

    def decorator(func):
        if skip_src and skip_dst:
            func.__doc__ += (
                "\n\n    Can be run with both src and dst set null in the config file."
            )
        elif skip_src:
            func.__doc__ += "\n\n    Can be run with a null src in the config file."
        elif skip_dst:
            func.__doc__ += "\n\n    Can be run with a null dst in the config file."
        else:
            func.__doc__ += (
                "\n\n    Requires both src and dst to be not null in the config file."
            )

        @wraps(func)
        async def wrapper(
            dc: str, db: Optional[str], json_mode: bool = False, **kwargs
        ):
            command_name = func.__name__.replace("_", "-")
            t0 = time.monotonic()

            try:
                if db is not None:
                    results = [
                        await func(
                            get_config_async(
                                db, dc, skip_src=skip_src, skip_dst=skip_dst
                            ),
                            **kwargs,
                        )
                    ]
                else:
                    results = await gather(
                        *[
                            func(fut, **kwargs)
                            async for fut in get_all_configs_async(
                                dc, skip_src=skip_src, skip_dst=skip_dst
                            )
                        ]
                    )
            except Exception as e:
                if json_mode:
                    duration_ms = int((time.monotonic() - t0) * 1000)
                    print(
                        _build_json_output(
                            command_name, dc, db, [], False, duration_ms, e
                        )
                    )
                    sys.exit(1)
                raise

            duration_ms = int((time.monotonic() - t0) * 1000)

            if json_mode:
                print(
                    _build_json_output(command_name, dc, db, results, True, duration_ms)
                )
                return

            if results_callback is None:
                return results
            return await results_callback(results)

        return wrapper

    if decorated_func is None:
        return decorator
    return decorator(decorated_func)


def add_command(app: Typer, command: Callable):
    """
    Helper which attaches a function to the given typer app. Merges
    the signature of the underlying implementation with standard arguments.
    This allows command options to be defined on the implementation and all
    commands in belt to share common arguments defined in one place.

    If a command is async, then it may be run on all dbs in a dc concurrently.
    Otherwise we assume it only makes sense to run it on one.
    """
    # Give typer the name of the actual implementing function
    name = command.__name__.replace("_", "-")

    # If async assume command can be run on a whole datacenter and make db optional
    if iscoroutinefunction(command):

        @app.command(name=name)
        def cmdwrapper(
            dc: str,
            db: Optional[str] = Argument(None),
            json: bool = Option(
                False,
                "--json",
                help="Output structured JSON instead of human-readable tables.",
            ),
            **kwargs,
        ):
            run(command(dc, db, json_mode=json, **kwargs))

    # Synchronous commands can only be run on one db at a time
    else:

        @app.command(name=name)
        def cmdwrapper(dc: str, db: str, **kwargs):
            command(db, dc, **kwargs)

    # remove the **kwargs since typer doesn't do anything with it
    wrap_signature = signature(cmdwrapper)
    wrap_params = wrap_signature.parameters.copy()
    wrap_params.popitem()

    # Remove any args without defaults from the implementation's signature
    # so we are left with only what typer interprets as options
    cmd_params = signature(command).parameters.copy()
    cmd_params_copy = cmd_params.copy()
    while (
        cmd_params_copy
        and cmd_params_copy.popitem(last=False)[1].default is Parameter.empty
    ):
        cmd_params.popitem(last=False)

    # The json_mode kwarg is handled by the wrapper, not the implementation.
    # Remove it so typer doesn't try to expose it as a duplicate option.
    cmd_params.pop("json_mode", None)

    # merge the arguments from the wrapper with the options from the implementation
    wrap_params.update(cmd_params)

    # set the signature for typer to read
    cmdwrapper.__signature__ = wrap_signature.replace(parameters=wrap_params.values())

    # the docstring on the implementation will be used as typer help
    cmdwrapper.__doc__ = command.__doc__
    if iscoroutinefunction(command):
        cmdwrapper.__doc__ += (
            "\n\n    If the db name is not given run on all dbs in the dc."
        )
