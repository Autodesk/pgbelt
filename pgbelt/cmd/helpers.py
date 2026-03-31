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
from typer import Argument
from typer import Option
from typer import Typer


T = TypeVar("T")


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

    detail = {}
    if results and len(results) == 1 and isinstance(results[0], dict):
        detail = results[0]
    elif results and all(isinstance(r, dict) for r in results):
        detail = {"databases": results}

    result = CommandResult(
        db=db or dc,
        dc=dc,
        command=command_name,
        success=success,
        duration_ms=duration_ms,
        error=cmd_error,
        detail=detail,
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
