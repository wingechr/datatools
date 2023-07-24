# coding: utf-8

import json
import logging
import sys

import click

from . import Storage, __version__
from .exceptions import DatatoolsException
from .load import read_uri, write_uri
from .storage import StorageServer
from .utils import parse_cli_metadata


@click.group()
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    default="debug",
    show_default=True,
)
@click.option("--location", "-d")
def main(ctx, loglevel, location):
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

    ctx.obj = Storage(location=location)


@main.command
@click.pass_obj
@click.argument("data_path")
@click.argument("file_path")
def data_get(storage: Storage, data_path: str, file_path: str):
    data = storage.data_get(data_path=data_path)
    if file_path == "-":
        file_path = None
    if not file_path:
        sys.stdout.buffer.write(data)
    else:
        write_uri(file_path, data)


@main.command
@click.pass_obj
@click.argument("data_path")
def data_delete(storage: Storage, data_path: str):
    storage.data_delete(data_path=data_path)


@main.command
@click.pass_obj
@click.argument("file_path")
@click.argument("data_path", required=False)
@click.option(
    "--hash_method", "-h", type=click.Choice(["md5", "sha256"]), default="md5"
)
def data_put(storage: Storage, file_path, data_path: str, hash_method: str):
    if file_path == "-":
        file_path = None
        data = sys.stdin.buffer.read()
        metadata = None
    else:
        data, _data_path, metadata = read_uri(file_path)

        if data_path is None:  # ! explicitly use is None, so we can manually set ""
            data_path = _data_path

    data_path = storage.data_put(
        data=data, data_path=data_path, hash_method=hash_method
    )

    if metadata:
        storage.metadata_put(data_path=data_path, metadata=metadata)

    print(data_path)


@main.command
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_path", required=False)
def metadata_get(storage: Storage, data_path, metadata_path):
    results = storage.metadata_get(data_path=data_path, metadata_path=metadata_path)
    print(json.dumps(results, indent=2, ensure_ascii=True))


@main.command
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_key_vals", nargs=-1, required=True)
def metadata_put(storage: Storage, data_path, metadata_key_vals):
    metadata = parse_cli_metadata(metadata_key_vals)
    storage.metadata_put(data_path=data_path, metadata=metadata)


@main.command
@click.pass_obj
@click.option("--port", "-p", type=int)
def serve(storage: Storage, port: int):
    server = StorageServer(storage=storage, port=port)
    server.serve_forever()


if __name__ == "__main__":
    try:
        main(prog_name="datatools")
    except DatatoolsException as exc:
        # format error in a way that we can read and decode again: <>
        logging.error(f"<{exc.__class__.__name__}: {str(exc)}>")
        sys.exit(1)
    except Exception:
        raise
