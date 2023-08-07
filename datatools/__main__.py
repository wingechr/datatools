# coding: utf-8

import json
import logging
import sys

import click

from . import GLOBAL_LOCATION, LOCAL_LOCATION, Storage, __version__
from .exceptions import DataDoesNotExists, DatatoolsException
from .resource import open_uri, write_uri
from .utils import as_uri, json_serialize, parse_cli_metadata


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


@main.command("data-get")
@click.pass_obj
@click.argument("data_path")
@click.argument("file_path")
def data_get(storage: Storage, data_path: str, file_path: str):
    if file_path in ("", "-"):
        file_path = None

    with storage.data_open(data_path=data_path) as file:
        if not file_path:
            # todo chunk
            sys.stdout.buffer.write(file.read())
        else:
            write_uri(file_path, file)


@main.command("data-delete")
@click.pass_obj
@click.argument("data_path")
def data_delete(storage: Storage, data_path: str):
    storage.data_delete(data_path=data_path)


@main.command("data-put")
@click.pass_obj
@click.option("--exist-ok", "-e", is_flag=True)
@click.argument("source")
@click.argument("data_path", required=False)
def data_put(storage: Storage, source, data_path: str = None, exist_ok=False):
    if source in ("", "-"):
        data = sys.stdin.buffer
        metadata = {}
    else:
        uri = as_uri(source=source)
        data, metadata = open_uri(uri)
        if data_path is None:
            data_path = uri
    logging.debug(data)
    norm_data_path = storage.data_put(data=data, data_path=data_path, exist_ok=exist_ok)
    if metadata:
        storage.metadata_set(data_path=norm_data_path, metadata=metadata)
    print(norm_data_path)


@main.command("metadata-get")
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_path", required=False)
def metadata_get(storage: Storage, data_path, metadata_path):
    results = storage.metadata_get(data_path=data_path, metadata_path=metadata_path)
    print(json.dumps(results, indent=2, ensure_ascii=True, default=json_serialize))


@main.command("metadata-put")
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_key_vals", nargs=-1, required=True)
def metadata_set(storage: Storage, data_path, metadata_key_vals):
    metadata = parse_cli_metadata(metadata_key_vals)
    storage.metadata_set(data_path=data_path, metadata=metadata)


@main.command("data-exists")
@click.pass_obj
@click.argument("data_path")
def data_exists(storage: Storage, data_path: str):
    norm_data_path = storage.data_exists(data_path=data_path)
    if not norm_data_path:
        raise DataDoesNotExists(data_path)
    print(norm_data_path)


if __name__ == "__main__":
    try:
        main(prog_name="datatools")
    except DatatoolsException as exc:
        logging.error(exc)
        sys.exit(1)
    except Exception as exc:
        logging.error(exc)
        raise
