from collections.abc import Awaitable
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
import ctypes
import sys
import platform
from importlib.resources import files


def get_lib_filename():
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "darwin":
        if machine == "x86_64" or machine == "amd64":
            return "pgcompare_darwin_amd64.so"
        elif machine == "arm64":
            return "pgcompare_darwin_arm64.so"
    elif system == "linux":
        if machine == "x86_64" or machine == "amd64":
            return "pgcompare_linux_amd64.so"
        elif machine == "aarch64" or machine == "arm64":
            return "pgcompare_linux_arm64.so"
    elif system == "windows":
        if machine == "x86_64" or machine == "amd64":
            return "pgcompare_windows_amd64.dll"
        elif machine == "aarch64" or machine == "arm64":
            return "pgcompare_windows_arm64.dll"
    raise RuntimeError(f"Unsupported platform: {system} {machine}")


lib_filename = get_lib_filename()
so_path = files("pgbelt").joinpath(lib_filename)
lib = ctypes.CDLL(str(so_path))


@run_with_configs
async def compare(config_future: Awaitable[DbupgradeConfig]) -> None:
    conf = await config_future
    file_location = conf.file
    file_location_bytes = ctypes.c_char_p(file_location.encode("utf-8"))
    lib.Run.argtypes = [ctypes.c_char_p]
    lib.Run.restype = None
    lib.Run(file_location_bytes)


COMMANDS = [compare]
