import json
import logging
import os
import re

import appdirs
import click
import coloredlogs

from . import __app_name__, __version__
from .storage import DataIndex


@click.group("main")
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"]),
    default="info",
)
@click.option("is_global", "--global", "-g", is_flag=True, help="use global repository")
@click.option("--data-location", "-d", help="change the default location")
def main(ctx, loglevel, is_global, data_location):
    """Script entry point."""

    # setup logging
    if isinstance(loglevel, str):
        loglevel = getattr(logging, loglevel.upper())
    coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
    coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    coloredlogs.DEFAULT_FIELD_STYLES = {"asctime": {"color": None}}
    coloredlogs.install(level=loglevel)

    def get_data_location(is_global):
        if is_global:
            path = appdirs.user_data_dir(
                __app_name__, appauthor=None, version=None, roaming=False
            )
            path += "/data"
        else:
            path = "."
        return path

    data_location = data_location or get_data_location(is_global)
    data_location = os.path.abspath(data_location)
    os.makedirs(data_location, exist_ok=True)

    ctx.obj = ctx.with_resource(DataIndex(data_location))


@main.command("list")
@click.option("regexp", "-r", help="regexp pattern")
@click.pass_obj
def list(index: DataIndex, regexp):
    regexp = re.compile(regexp or ".*")

    for res in index._data["resources"]:
        if regexp.match(res["path"]):
            res = json.dumps(res, indent=2)
            print(res)


@main.command("check")
@click.pass_obj
@click.option("--fix", "-f", is_flag=True, help="fix problems")
@click.option("--delete", "-d", is_flag=True, help="delete index entries for missing")
@click.option("--hash", "-h", is_flag=True, help="check hashes")
def check(index: DataIndex, fix, delete, hash):
    index.check(fix, delete, hash)


@main.command("download")
@click.pass_obj
@click.option("--force", "-f", is_flag=True, help="overwrite existing")
@click.argument("uri")
def download(index: DataIndex, uri, force):
    index.download(uri, force=force)


if __name__ == "__main__":
    main(prog_name=__app_name__)
