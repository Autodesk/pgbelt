from collections.abc import Awaitable
from pgbelt.cmd.helpers import run_with_configs
from pgbelt.config.models import DbupgradeConfig
import platform
import subprocess



@run_with_configs
async def compare(config_future: Awaitable[DbupgradeConfig]) -> None:
    conf = await config_future
    print(conf)
    # system_platform = platform.system()
    # if system_platform == "Windows":
    #     binary_path = "pg-compare/pg-compare-windows.exe"
    # elif system_platform == "Darwin":  # macOS
    #     binary_path = "pg-compare/pg-compare-macos"
    # elif system_platform == "Linux":
    #     binary_path = "pg-compare/pg-compare-linux"
    # else:
    #     raise RuntimeError(f"Unsupported platform: {system_platform}")

    # try:
    #     result = subprocess.run([binary_path, *args], check=True, capture_output=True, text=True)
    #     return {"status": "success", "output": result.stdout}
    # except subprocess.CalledProcessError as e:
    #     return {"status": "error", "output": e.stderr}


COMMANDS = [compare]
