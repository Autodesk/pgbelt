from typer import Typer

from pgbelt.cmd import add_commands

app = Typer(help="A tool to help manage postgres data migrations.")
add_commands(app)


if __name__ == "__main__":
    app()
