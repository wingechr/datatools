import logging
from types import SimpleNamespace

import click
import coloredlogs

from datatools import Repository, __app_name__, __version__


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
@click.option("--repository-location", "-r")
@click.argument("uri")
def resource(ctx, loglevel, repository_location=None, uri=None):
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

    ctx.obj = SimpleNamespace()

    ctx.obj.repository = Repository(location=repository_location)

    ctx.with_resource(ctx.obj.repository)

    ctx.obj.resource = ctx.obj.repository[uri]
    logging.debug(ctx.obj.resource)


# @resource.command("meta")
# @click.pass_context
# def repo_res_meta(ctx):
#    logging.debug(ctx.obj.resource.metadata)
#
#    ctx.obj.resource.metadata["a"] = 1


@resource.command("download")
@click.pass_context
def repo_res_download(ctx):
    ctx.obj.resource.download()


if __name__ == "__main__":
    resource(prog_name=__app_name__)
