#!/usr/bin/env python3

import logging

import click
import coloredlogs

# https://coloredlogs.readthedocs.io/en/latest/api.html#changing-the-date-time-format
coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
coloredlogs.DEFAULT_FIELD_STYLES = {
    "asctime": {"color": "black", "bold": True},  # gray
    "levelname": {"color": "black", "bold": True},  # gray
}
coloredlogs.DEFAULT_LEVEL_STYLES = {
    "debug": {"color": "black", "bold": True},  # gray
    "info": {"color": "white"},
    "warning": {"color": "yellow"},
    "error": {"color": "red", "bold": 10},
}


def create_main(name=None, version=None):
    @click.group(name)
    @click.pass_context
    @click.version_option(version)
    @click.option(
        "--loglevel",
        "-l",
        type=click.Choice(["debug", "info", "warning", "error"]),
        default="info",
    )
    def main(ctx, loglevel):
        """Script entry point."""
        if isinstance(loglevel, str):  # e.g. 'debug'/'DEBUG' -> logging.DEBUG
            loglevel = getattr(logging, loglevel.upper())
        coloredlogs.install(level=loglevel)
        ctx.ensure_object(dict)

    return main
