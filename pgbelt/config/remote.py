from importlib import import_module
from json import JSONDecodeError
from logging import Logger
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from typing import Optional

from aiofiles import open as aopen
from pydantic import BaseModel
from pydantic import ValidationError


def remote_conf_path(db: str, dc: str) -> str:
    return f"remote-configs/{dc}/{db}/config.json"


class RemoteConfigError(Exception):
    """
    raised by the resolve method of a resolver when it could not retrieve
    configuration due to an error.
    """

    pass


class RemoteConfigDefinition(BaseModel):
    """
    Pydantic model representing the contents of a remote-config json
    before we have the actual resolver to put it in. So the only required
    key is `resolver_path`. The rest are passed into the resolver model.
    """

    resolver_path: str

    class Config:
        extra = "allow"


class BaseResolver(BaseModel):
    """
    Remote configuration resolvers must subclass this.

    db: str the database name
    dc: str the datacenter name
    skip_src: bool don't retrieve the source db configuration
    skip_dst: bool don't retrieve the destination db configuration
    logger: Logger your resolver should log through this logger

    Your resolver subclass will be a pydantic model. Any attributes you define
    other than the ones already in here will come from your remote config files.
    """

    db: str
    dc: str
    skip_src: bool
    skip_dst: bool
    logger: Logger

    class Config:
        arbitrary_types_allowed = True
        extra = "ignore"

    async def resolve(self) -> Optional[DbupgradeConfig]:
        """
        Called to retrieve the configuration from wherever your resolver gets it.
        Return configuration as a DbupgradeConfig.

        If you should have been able to get configuration but encountered an error
        then throw a RemoteConfigError. If there was no config and no error return None
        """
        raise NotImplementedError


async def load_remote_conf_def(
    config_file: str, logger: Logger
) -> Optional[RemoteConfigDefinition]:
    try:
        logger.debug(f"Reading remote config definition from file {config_file}")
        async with aopen(config_file, mode="r") as f:
            raw_json = await f.read()

        return RemoteConfigDefinition.parse_raw(raw_json)
    except FileNotFoundError:
        logger.error(f"No remote config definition exists at {config_file}")
    except JSONDecodeError:
        logger.error(f"Remote config definition in {config_file} was malformed JSON.")
    except ValidationError:
        logger.error(f"Remote config definition in {config_file} was not valid.")


async def resolve_remote_config(
    db: str, dc: str, skip_src: bool = False, skip_dst: bool = False
) -> Optional[DbupgradeConfig]:
    """
    Loads the referenced remote configuration json file, tries to import the
    specified resolver class, executes its resolve method, and returns the
    resulting DbupgradeConfig or None if there was an error
    """

    # set up the logger
    logger = get_logger(db, dc, "remote-config")

    # load the remote config from the json file
    definition = await load_remote_conf_def(remote_conf_path(db, dc), logger)

    if definition is None:
        return None

    module, classname = definition.resolver_path.rsplit(".", 1)

    try:
        resolver_module = import_module(module)
    except ModuleNotFoundError:
        logger.error(f"Could not find module {module}")
        return None

    try:
        resolver_class = getattr(resolver_module, classname)
    except AttributeError:
        logger.error(f"Config resolver class {classname} does not exist in {module}")
        return None

    if not issubclass(resolver_class, BaseResolver):
        logger.error(
            f"Config resolver class {classname} from {module} is not a config resolver"
        )
        return None

    try:
        resolver_dict = definition.dict()

        resolver_dict.update(
            {
                "db": db,
                "dc": dc,
                "skip_src": skip_src,
                "skip_dst": skip_dst,
                "logger": logger.getChild(classname),
            }
        )

        resolver = resolver_class(**resolver_dict)
    except ValidationError:
        logger.error(
            f"Remote config definition for {db} {dc} was not valid for {resolver_class.__name__}"
        )
        return None

    try:
        config = await resolver.resolve()
    except NotImplementedError:
        logger.error(
            f"Config resolver class {classname} from {module} does not implement resolve"
        )
        return None
    except RemoteConfigError:
        logger.error(f"Failed to resolve remote configuration for {db} {dc}")
        return None
    except ValidationError:
        logger.error(
            f"Configuration for {db} {dc} resolved by {resolver_class.__name__} was not a valid DbupgradeConfig"
        )
        return None

    logger.info(
        f"Successfully resolved remote configuration for {db} {dc} using {resolver_class.__name__}"
    )

    return config
