# coding: utf-8

import logging
import sys

import click

from . import __version__
from .constants import GLOBAL_LOCATION, LOCAL_LOCATION
from .exceptions import DatatoolsException
from .storage import Storage


@click.group()
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    default="info",
    show_default=True,
)
@click.option("--location", "-d")
@click.option("--global-location", "-g", is_flag=True)
def main(ctx, loglevel, location, global_location):
    """Script entry point."""
    # setup default logging
    loglevel = getattr(logging, loglevel.upper())
    format = "[%(asctime)s %(levelname)7s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=loglevel, format=format, datefmt=datefmt)
    # setup color logging
    try:
        import coloredlogs

        coloredlogs.DEFAULT_LOG_FORMAT = format
        coloredlogs.DEFAULT_DATE_FORMAT = datefmt
        coloredlogs.DEFAULT_FIELD_STYLES = {"asctime": {"color": None}}
        coloredlogs.install(level=loglevel)
    except ModuleNotFoundError:
        pass

    if location and global_location:
        raise DatatoolsException("location and global-location are mutually exclusive")
    if global_location:
        location = GLOBAL_LOCATION
    if not location:
        location = LOCAL_LOCATION
    ctx.obj = Storage(location=location)


@main.command("check")
@click.pass_obj
@click.option("--fix", "-f", is_flag=True)
def check(storage: Storage, fix):
    storage.check(fix=fix)


@main.command("search")
@click.pass_obj
@click.argument("patterns", nargs=-1)
def search(storage: Storage, patterns):
    for result in storage.search(*patterns):
        print(result)


@main.group("res")
@click.pass_context
@click.argument("source_uri")
def resource(ctx, source_uri):
    storage = ctx.obj
    resource = storage.resource(source_uri=source_uri)
    ctx.obj = resource


@resource.command("save")
@click.pass_obj
def resource_save(resource: Storage):
    resource.write(exist_ok=True)
    print(resource.name)


if __name__ == "__main__":
    try:
        main(prog_name="datatools")
    except DatatoolsException as exc:
        logging.error(f"{exc.__class__.__name__}: {exc}")
        sys.exit(1)
    except Exception as exc:
        logging.error(exc)
        raise
