from rich.table import Table


# Class for rich text Table arguments. This is done because the Table object in rich
# is not easily testable (having trouble accessing the rows and columns attributes)
class RichTableArgs:

    # Make attributes public for easy access
    title: str
    columns: list[str]
    rows: list[list[str]]

    def __init__(
        self, title: str, columns: list[str] = None, rows: list[list[str]] = None
    ):
        self.title = title
        self.columns = columns
        self.rows = rows


# Method to take a RichTableArgs object and return a rich Table object
def build_rich_table(args: RichTableArgs) -> Table:
    table = Table(title=args.title)
    for column in args.columns:
        table.add_column(column)
    for row in args.rows:
        table.add_row(*row)
    return table
