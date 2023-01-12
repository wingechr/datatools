import logging
import sys

import click
import coloredlogs

import datatools


@click.group()
@click.pass_context
@click.version_option(datatools.__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"]),
    default="info",
)
def main(ctx, loglevel):
    """Script entry point."""
    ctx.ensure_object(dict)
    # setup logging
    if isinstance(loglevel, str):
        loglevel = getattr(logging, loglevel.upper())
    coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
    coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    coloredlogs.DEFAULT_FIELD_STYLES = {"asctime": {"color": None}}
    coloredlogs.install(level=loglevel)


@main.command
@click.pass_context
@click.argument("file_path")
def validate(ctx, file_path):
    datatools.cli_validate(file_path=file_path)


if __name__ == "__main__":
    try:
        main(prog_name="datatools")
    except Exception as exc:
        logging.error(exc)
        sys.exit(1)
