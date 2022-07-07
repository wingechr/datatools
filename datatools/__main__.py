#!/usr/bin/env python3

import logging

import click
import coloredlogs

import datatools

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
    if isinstance(loglevel, str):
        loglevel = getattr(logging, loglevel.upper())
    coloredlogs.install(level=loglevel)
    ctx.ensure_object(dict)


@main.group(name="validate")
@click.pass_context
def validate(ctx):
    pass


@validate.command(name="json")
@click.pass_context
@click.argument("json_file", type=click.types.Path(exists=True))
@click.argument("schema_file", type=click.types.Path(exists=True), required=False)
def validate_json(ctx, json_file: object, schema_file=None):
    json = datatools.utils.json.load(json_file)
    schema = datatools.utils.json.load(schema_file) if schema_file else None
    datatools.validate_json(json, schema)


if __name__ == "__main__":
    main(prog_name="datatools")
