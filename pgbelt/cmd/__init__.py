from importlib import import_module
from pkgutil import walk_packages

from pgbelt import cmd
from pgbelt.cmd.helpers import add_command as _add_command

COMMANDS = []

# discover all commands in any module in this directory
for _, modname, _ in walk_packages(cmd.__path__):
    mod = import_module(f"{cmd.__name__}.{modname}")
    COMMANDS += getattr(mod, "COMMANDS", [])


def add_commands(app):
    for command in COMMANDS:
        _add_command(app, command)
