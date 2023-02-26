import logging
from functools import cached_property
from pathlib import Path

import click
import coloredlogs

from datatools import __app_name__, __version__
from datatools.classes import MainContext


class Repository(MainContext):
    def __init__(self, base_dir):
        self.base_path = Path(base_dir).absolute()
        logging.debug(f"base_dir: {self.base_path.as_posix()}")

    def get_location(self, location):
        path = Path(location)
        if path.is_absolute():
            return location
        path = self.base_path.joinpath(path)
        return path.as_posix()

    def resource(self, location):
        location = self.get_location(location)
        return Resource(self, location)


class MetadataStorage:
    def __init__(self, index_location):
        self.index_location = index_location
        logging.debug(f"index_location: {self.index_location}")

    def get_resource(self, path):
        pass


class ResourceMetadata:
    """Proxy object for resource inside a metadata storage"""

    def __init__(self, resource):
        self.resource = resource
        self.metadata_storage = self.get_metadata_storage(resource.location)

    @classmethod
    def get_metadata_storage(cls, location):

        return MetadataStorage(location + ".metadata.json")

    def get(self, key, value_default=None):
        return value_default

    def set(self, key, value=None):
        pass

    def check(self, key, value=None):
        if value is None:
            value = self.get(key)

    def update(self, key):
        pass


class Resource:
    def __init__(self, repository, location):
        self.repository = repository
        self.location = location

    def __enter__(self):
        # logging.debug(f"enter {self}")
        return self

    def __exit__(self, *args):
        # logging.debug(f"exit {self}")
        pass

    @cached_property
    def metadata(self):
        return ResourceMetadata(self)

    @cached_property
    def metadata_storage(self):
        return MetadataStorage(self.location + ".metadata.json")

        return ResourceMetadata(self)


@click.group("main")
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(
        ["debug", "info", "warning", "error", "d", "i", "w", "e"], case_sensitive=False
    ),
    default="d",
)
@click.option("--base-dir", "-b", default=".")
def main(ctx, loglevel, base_dir):
    """Script entry point."""

    # setup logging
    loglevel = loglevel.lower()
    loglevel = {"d": "debug", "i": "info", "w": "warning", "e": "error"}.get(
        loglevel, loglevel
    )
    loglevel = getattr(logging, loglevel.upper())

    coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
    coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    coloredlogs.DEFAULT_FIELD_STYLES = {"asctime": {"color": None}}
    coloredlogs.install(level=loglevel)
    ctx.obj = ctx.with_resource(Repository(base_dir=base_dir))


@main.group("meta")
@click.pass_context
@click.argument("location")
def meta(ctx, location):
    repository = ctx.obj
    resource = repository.resource(location)
    ctx.obj = resource.metadata


@meta.command("get")
@click.pass_obj
@click.argument("key")
@click.argument("value-default", required=False)
def meta_get(resource_metdata, key, value_default=None):
    val = resource_metdata.get(key, value_default)
    print(val)


@meta.command("set")
@click.pass_obj
@click.argument("key")
@click.argument("value", required=False)
def meta_set(resource_metdata, key, value=None):
    resource_metdata.set(key, value)


@meta.command("check")
@click.pass_obj
@click.argument("key")
@click.argument("value", required=False)
def meta_check(resource_metdata, key, value=None):
    resource_metdata.check(key, value)


@meta.command("update")
@click.pass_obj
@click.argument("key")
def meta_update(resource_metdata, key):
    resource_metdata.update(key)


if __name__ == "__main__":
    main(prog_name=__app_name__)
