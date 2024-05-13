# coding: utf-8

import json
import logging
import sys

import click

from datatools import __version__
from datatools.constants import GLOBAL_LOCATION, LOCAL_LOCATION
from datatools.exceptions import DatatoolsException
from datatools.storage import Metadata, Resource, Storage
from datatools.utils import as_uri, json_serialize, parse_cli_metadata


@click.group(name="datatools")
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


@main.command("search")
@click.pass_obj
@click.argument("patterns", nargs=-1)
def search(storage: Storage, patterns):
    for res in storage.find_resources(*patterns):
        print(res)


@main.group("res")
@click.pass_context
@click.argument("path")
@click.option("--name", "-n")
def resource(ctx, path, name=None):
    storage = ctx.obj
    source_uri = as_uri(path)
    resource = storage.resource(source_uri, name=name)
    ctx.obj = resource


@resource.command("save")
@click.pass_obj
def resource_download_save(resource: Resource):
    if resource.exists():
        logging.info("Already saved")
    else:
        resource.save()
    print(resource.name)


@resource.group("meta")
@click.pass_context
def resource_meta(ctx):
    resource = ctx.obj
    ctx.obj = resource.metadata


@resource_meta.command("query")
@click.pass_obj
@click.argument("key", required=False)
def resource_meta_query(metadata: Metadata, key=None):
    result = metadata.query(key)
    result_str = json.dumps(result, indent=2, ensure_ascii=True, default=json_serialize)
    print(result_str)


@resource_meta.command("update", help="Multiple key=value pairs")
@click.pass_obj
@click.argument("metadata_key_vals", nargs=-1)
def resource_meta_update(metadata: Metadata, metadata_key_vals):
    new_metadata = parse_cli_metadata(metadata_key_vals)
    metadata.update(new_metadata)


def _recursive_help(cmd=main, parent=None, path=None) -> str:
    path = path or []
    ctx = click.core.Context(cmd, info_name=cmd.name, parent=parent)

    result = ""

    if path:  # not root
        path_s = " ".join(path)
        result += "\n\n## " + path_s + "\n"

    result += cmd.get_help(ctx) + "\n"

    commands = getattr(cmd, "commands", {})
    for name, group_or_command in commands.items():
        result += _recursive_help(group_or_command, ctx, (path or ()) + (name,))

    return result


@main.command("help-all")
def help_all():
    print(_recursive_help())


if __name__ == "__main__":
    try:
        main()
    except DatatoolsException as exc:
        logging.error(f"{exc.__class__.__name__}: {exc}")
        sys.exit(1)
    except Exception as exc:
        logging.error(exc)
        raise
