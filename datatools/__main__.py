#!/usr/bin/env python3

import logging

import click
import coloredlogs

import datatools
from datatools.location import location

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


@validate.command(name="jsonschema")
@click.pass_context
@click.argument("json-file", type=click.types.Path(exists=True))
@click.argument("schema", required=False)
def validate_jsonschema(ctx, json_file: object, schema=None):
    if schema:
        schema = location(schema).read(as_json=True)
    else:
        schema = True  # get from json_file $schema
    location(json_file).read(as_json=True, json_schema=schema)


@main.command(name="download")
@click.pass_context
@click.argument("source-uri")
@click.argument("target-file-path", type=click.types.Path(exists=False))
@click.option("--overwrite", "-o", is_flag=True)
def download(ctx, source_uri, target_file_path, overwrite=False):
    report = location(target_file_path).write(
        location(source_uri).read(), overwrite=overwrite
    )
    print(report)


if __name__ == "__main__":
    main(prog_name="datatools")
