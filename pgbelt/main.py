from pgbelt.cmd import add_commands
from typer import Typer

app = Typer(help="A tool to help manage postgres data migrations.")
add_commands(app)


if __name__ == "__main__":
    app()
