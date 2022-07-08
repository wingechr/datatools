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


@main.group(name="json")
@click.pass_context
def json(ctx):
    pass


@json.command(name="validate")
@click.pass_context
@click.argument("json-file", type=click.types.Path(exists=True))
@click.option("--schema-file", "-s", type=click.types.Path(exists=True), default=None)
def json_validate(ctx, json_file: object, schema_file=None):
    json = datatools.utils.json.load(json_file)
    schema = datatools.utils.json.load(schema_file) if schema_file else None
    datatools.utils.json.validate(json, schema)


@main.command(name="hash")
@click.pass_context
@click.argument("file_path", type=click.types.Path(exists=True))
@click.option("--method", "-m", type=str, default="sha256")
def hash(ctx, file_path, method):
    print(datatools.utils.byte.hash(file_path, method=method))


@main.command(name="download")
@click.pass_context
@click.argument("source_uri")
@click.argument("target_file_path", type=click.types.Path(exists=False))
def download(ctx, source_uri, target_file_path):
    datatools.utils.http.download(source_uri, target_file_path)


if __name__ == "__main__":
    main(prog_name="datatools")
