import logging

import click
import coloredlogs

from datatools import __app_name__, __version__, conf
from datatools.classes import get_resource_handler


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
@click.option("--cache-dir", "-d")
def main(ctx, loglevel, cache_dir=None):
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

    if cache_dir:
        conf.cache_dir = cache_dir

    ctx.with_resource(conf.exit_stack)
    ctx.obj = {}


@main.group("meta")
@click.pass_context
@click.argument("location")
def meta(ctx, location):
    res = get_resource_handler(location)

    logging.debug(f"Location: {res.location}, exists={res.exists}")
    logging.debug(f"Index: {res.metadata.index_location}: {res.metadata.relative_path}")

    ctx.obj["resource_metadata"] = res.metadata


@meta.command("get")
@click.pass_context
@click.argument("key", required=False)
@click.argument("value-default", required=False)
def meta_get(ctx, key, value_default=None):
    val = ctx.obj["resource_metadata"].get(key, value_default)
    print(val)


@meta.command("set")
@click.pass_context
@click.argument("key")
@click.argument("value", required=False)
def meta_set(ctx, key, value=None):
    ctx.obj["resource_metadata"].set(key, value)


@meta.command("check")
@click.pass_context
@click.argument("key")
@click.argument("value", required=False)
def meta_check(ctx, key, value=None):
    ctx.obj["resource_metadata"].check(key, value)


@meta.command("update")
@click.pass_context
@click.argument("key")
def meta_update(ctx, key):
    ctx.obj["resource_metadata"].update(key)


if __name__ == "__main__":
    main(prog_name=__app_name__)
