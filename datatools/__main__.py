# coding: utf-8

import logging
import sys

import click

from . import GLOBAL_LOCATION, LOCAL_LOCATION, Storage, __version__
from .exceptions import DatatoolsException

# from .utils import as_uri, json_serialize, parse_cli_metadata


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


@main.command("save")
@click.pass_obj
@click.argument("uri")
@click.argument("name", required=False)
def data_put(storage: Storage, uri, name: str = None):
    res = storage.resource(uri=uri, name=name)
    res._save_if_not_exist()
    print(res.name)


if __name__ == "__main__":
    try:
        main(prog_name="datatools")
    except DatatoolsException as exc:
        logging.error(exc)
        sys.exit(1)
    except Exception as exc:
        logging.error(exc)
        raise
