import asyncio
from functools import partial
from functools import wraps
from os import listdir as _listdir
from os import makedirs as _makedirs
from os.path import isdir as _isdir
from os.path import isfile as _isfile


def make_async(sync_func):
    @wraps(sync_func)
    async def do_async(*args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(
            None, partial(sync_func, *args, **kwargs)
        )

    return do_async


listdir = make_async(_listdir)
makedirs = make_async(_makedirs)
isdir = make_async(_isdir)
isfile = make_async(_isfile)
