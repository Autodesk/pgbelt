import logging
from os import getenv
from os import makedirs


FORMATTER = "{name}:{levelname} {message}"

# if this module is ever imported we set up the root logger to log to stderr
root_level = int(getenv("LOG_LEVEL", logging.DEBUG))
root_handler = logging.StreamHandler()
root_handler.setFormatter(logging.Formatter(FORMATTER, style="{"))
root_handler.setLevel(root_level)
root_logger = logging.getLogger("dbup")
root_logger.setLevel(root_level)
root_logger.addHandler(root_handler)


def log_file_dir(db: str, dc: str) -> str:
    return f"logs/{db}/{dc}"


def log_file_path(db: str, dc: str) -> str:
    return f"logs/{db}/{dc}/logs.txt"


def get_logger(db: str, dc: str, kind: str = "") -> logging.Logger:
    # When we set up a logger for that db that emits to a file
    logger = logging.getLogger(f"dbup.{db}.{dc}")
    if not logger.handlers:
        try:
            makedirs(log_file_dir(db, dc))
        except FileExistsError:
            pass

        handler = logging.FileHandler(log_file_path(db, dc), mode="w")
        handler.setFormatter(logging.Formatter(FORMATTER, style="{"))
        # always log everything to the file
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

    # if you pass kind then you can get a logger for a specific thing,
    # and your logs will end up annotated with the kind
    return logging.getLogger(f"dbup.{db}.{dc}.{kind}") if kind else logger
