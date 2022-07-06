from asyncio import gather
from asyncio import run
from functools import wraps
from inspect import iscoroutinefunction
from inspect import Parameter
from inspect import signature
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional
from typing import TypeVar

from typer import Argument
from typer import Typer

from pgbelt.config import get_all_configs_async
from pgbelt.config import get_config_async


T = TypeVar("T")


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

        # The name, docstring, and signature of the implementation is preserved. Important for add_command
        @wraps(func)
        async def wrapper(dc: str, db: Optional[str], **kwargs):
            # If db is specified we only want to run on one of them
            if db is not None:
                results = [
                    await func(
                        get_config_async(db, dc, skip_src=skip_src, skip_dst=skip_dst),
                        **kwargs
                    )
                ]
            else:
                # if the db is not provided run on all the dbs in the dc
                results = await gather(
                    *[
                        func(fut, **kwargs)
                        async for fut in get_all_configs_async(
                            dc, skip_src=skip_src, skip_dst=skip_dst
                        )
                    ]
                )

            # Call the callback if provided.
            if results_callback is None:
                return results
            return await results_callback(results)

        return wrapper

    # makes either @decorator or @decorator(...) work to decorate a function
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
        def cmdwrapper(dc: str, db: Optional[str] = Argument(None), **kwargs):
            run(command(dc, db, **kwargs))

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
