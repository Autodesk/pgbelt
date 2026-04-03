from importlib import import_module
from json import JSONDecodeError
from logging import Logger
from typing import Optional  # noqa: F401 # Needed until tiangolo/typer#522 is fixed)

from aiofiles import open as aopen
from pgbelt.config.models import DbConfig
from pgbelt.config.models import DbupgradeConfig
from pgbelt.util.logs import get_logger
from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import model_validator


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
    Pydantic model representing the contents of a remote-config json.

    Supports two modes:

    - Legacy: ``resolver_path`` points to a BaseResolver that returns
      DbupgradeConfig
    - Per-side: ``src_resolver_path`` and/or ``dst_resolver_path`` point
      to BaseSideResolver classes that each return a DbConfig.
      Resolver-specific config is scoped under
      ``src_resolver_config`` / ``dst_resolver_config`` dicts.
    """

    resolver_path: Optional[str] = None
    src_resolver_path: Optional[str] = None
    dst_resolver_path: Optional[str] = None
    src_resolver_config: Optional[dict] = None
    dst_resolver_config: Optional[dict] = None

    tables: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    schema_name: Optional[str] = "public"
    exclude_users: Optional[list[str]] = None
    exclude_patterns: Optional[list[str]] = None

    class Config:
        extra = "allow"

    @model_validator(mode="after")
    def validate_resolver_paths(self):
        has_legacy = self.resolver_path is not None
        has_per_side = (
            self.src_resolver_path is not None
            or self.dst_resolver_path is not None
        )
        if not has_legacy and not has_per_side:
            raise ValueError(
                "Must specify either 'resolver_path' or at least one of "
                "'src_resolver_path' / 'dst_resolver_path'"
            )
        if has_legacy and has_per_side:
            raise ValueError(
                "Cannot mix 'resolver_path' with "
                "'src_resolver_path' / 'dst_resolver_path'"
            )
        return self

    @property
    def is_per_side(self) -> bool:
        return self.resolver_path is None


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


class BaseSideResolver(BaseModel):
    """
    Per-side remote configuration resolver. Returns a DbConfig for one side
    (source or destination). Pglogical password management is per-resolver --
    each resolver generates or retrieves its own pglogical_user.pw.

    db: str the database pair name
    dc: str the datacenter / project name
    logger: Logger your resolver should log through this logger

    Your resolver subclass will be a pydantic model. Any attributes you define
    other than the ones here will come from src_resolver_config / dst_resolver_config
    in the remote config JSON.
    """

    db: str
    dc: str
    logger: Logger

    class Config:
        arbitrary_types_allowed = True
        extra = "ignore"

    async def resolve(self) -> Optional[DbConfig]:
        """
        Fetch credentials and return a DbConfig for one side of a migration.

        The returned DbConfig must include a real pglogical_user.pw (not a
        placeholder). The framework will not overwrite it.

        Raise RemoteConfigError on retrieval failures.
        Return None if no config exists and that is not an error.
        """
        raise NotImplementedError


def _import_class(dotted_path: str, logger: Logger):
    module_path, classname = dotted_path.rsplit(".", 1)
    try:
        mod = import_module(module_path)
    except ModuleNotFoundError:
        logger.error(f"Could not find module {module_path}")
        return None
    try:
        cls = getattr(mod, classname)
    except AttributeError:
        logger.error(f"Class {classname} does not exist in {module_path}")
        return None
    return cls


async def _resolve_side(
    resolver_path: str,
    resolver_config: dict,
    db: str,
    dc: str,
    logger: Logger,
    side_label: str,
) -> Optional[DbConfig]:
    cls = _import_class(resolver_path, logger)
    if cls is None:
        return None

    if not issubclass(cls, BaseSideResolver):
        logger.error(
            f"{side_label} resolver {cls.__name__} is not a BaseSideResolver"
        )
        return None

    try:
        resolver = cls(
            db=db,
            dc=dc,
            logger=logger.getChild(cls.__name__),
            **resolver_config,
        )
    except ValidationError:
        logger.error(
            f"{side_label} resolver config was not valid for {cls.__name__}"
        )
        return None

    try:
        return await resolver.resolve()
    except NotImplementedError:
        logger.error(f"{cls.__name__} does not implement resolve")
        return None
    except RemoteConfigError as e:
        logger.error(f"{side_label} resolver failed: {e}")
        return None
    except ValidationError:
        logger.error(
            f"{side_label} resolver {cls.__name__} returned invalid DbConfig"
        )
        return None


async def _resolve_per_side(
    definition: RemoteConfigDefinition,
    db: str,
    dc: str,
    skip_src: bool,
    skip_dst: bool,
    logger: Logger,
) -> Optional[DbupgradeConfig]:
    src = None
    dst = None

    if not skip_src and definition.src_resolver_path:
        logger.info("Resolving source configuration...")
        src = await _resolve_side(
            definition.src_resolver_path,
            definition.src_resolver_config or {},
            db, dc, logger, "src",
        )
        if src is None:
            logger.error("Source resolver returned no config")
            return None
    elif not skip_src and not definition.src_resolver_path:
        logger.info("No src_resolver_path specified, skipping source resolution")

    if not skip_dst and definition.dst_resolver_path:
        logger.info("Resolving destination configuration...")
        dst = await _resolve_side(
            definition.dst_resolver_path,
            definition.dst_resolver_config or {},
            db, dc, logger, "dst",
        )
        if dst is None:
            logger.error("Destination resolver returned no config")
            return None
    elif not skip_dst and not definition.dst_resolver_path:
        logger.info("No dst_resolver_path specified, skipping destination resolution")

    try:
        config = DbupgradeConfig(
            db=db,
            dc=dc,
            src=src,
            dst=dst,
            tables=definition.tables,
            sequences=definition.sequences,
            schema_name=definition.schema_name,
            exclude_users=definition.exclude_users,
            exclude_patterns=definition.exclude_patterns,
        )
    except ValidationError:
        logger.error(f"Assembled DbupgradeConfig for {db} {dc} is not valid")
        return None

    logger.info(f"Successfully resolved per-side configuration for {db} {dc}")
    return config


async def load_remote_conf_def(
    config_file: str, logger: Logger
) -> Optional[RemoteConfigDefinition]:
    try:
        logger.debug(f"Reading remote config definition from file {config_file}")
        async with aopen(config_file, mode="r") as f:
            raw_json = await f.read()

        return RemoteConfigDefinition.model_validate_json(raw_json)
    except FileNotFoundError:
        logger.error(f"No remote config definition exists at {config_file}")
    except JSONDecodeError:
        logger.error(f"Remote config definition in {config_file} was malformed JSON.")
    except ValidationError as e:
        logger.error(f"Remote config definition in {config_file} was not valid: {e}")


async def resolve_remote_config(
    db: str, dc: str, skip_src: bool = False, skip_dst: bool = False
) -> Optional[DbupgradeConfig]:
    """
    Loads the referenced remote configuration json file, tries to import the
    specified resolver class, executes its resolve method, and returns the
    resulting DbupgradeConfig or None if there was an error.

    Supports two modes:

    - Legacy (``resolver_path``): single BaseResolver returns complete
      DbupgradeConfig
    - Per-side (``src_resolver_path`` / ``dst_resolver_path``): two
      BaseSideResolver classes each return a DbConfig, framework
      assembles DbupgradeConfig
    """

    logger = get_logger(db, dc, "remote-config")

    definition = await load_remote_conf_def(remote_conf_path(db, dc), logger)

    if definition is None:
        return None

    if definition.is_per_side:
        return await _resolve_per_side(
            definition, db, dc, skip_src, skip_dst, logger
        )

    # Legacy single-resolver path
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
    except RemoteConfigError as e:
        logger.error(
            f"Failed to resolve remote configuration for {db} {dc}. RemoteConfigError {e}"
        )
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
