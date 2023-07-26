# coding: utf-8

import json
import logging
import sys

import click

from . import Storage, __version__
from .exceptions import DataDoesNotExists, DataExists, DatatoolsException
from .load import read_uri, write_uri
from .storage import StorageServer
from .utils import as_uri, parse_cli_metadata, uri_to_data_path


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


@main.command("data-get")
@click.pass_obj
@click.argument("data_path")
@click.argument("file_path")
def data_get(storage: Storage, data_path: str, file_path: str):
    data = storage._data_get(norm_data_path=data_path)
    if file_path == "-":
        file_path = None
    if not file_path:
        sys.stdout.buffer.write(data)
    else:
        write_uri(file_path, data)


@main.command("data-delete")
@click.pass_obj
@click.argument("data_path")
def data_delete(storage: Storage, data_path: str):
    storage._data_delete(norm_data_path=data_path)


@main.command("data-put")
@click.pass_obj
@click.option("--exist-ok", "-e", is_flag=True)
@click.argument("source")
@click.argument("data_path", required=False)
def data_put(storage: Storage, source, data_path: str = None, exist_ok=False):
    if source == "-":
        source = None
        data = sys.stdin.buffer.read()
        metadata = None
    else:
        uri = as_uri(source)
        if data_path is None:  # ! explicitly use is None, so we can manually set ""
            data_path = uri_to_data_path(uri)
            norm_data_path = storage._data_exists(norm_data_path=data_path)
            if norm_data_path:
                if not exist_ok:
                    raise DataExists(data_path)
                logging.info(f"Already in storage: {data_path}")
                print(norm_data_path)
                return

        data, metadata = read_uri(uri)

    data_path = storage._data_put(
        data=data, norm_data_path=data_path, exist_ok=exist_ok
    )

    if metadata:
        storage._metadata_put(norm_data_path=data_path, metadata=metadata)

    print(data_path)


@main.command("metadata-get")
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_path", required=False)
def metadata_get(storage: Storage, data_path, metadata_path):
    results = storage._metadata_get(
        norm_data_path=data_path, metadata_path=metadata_path
    )
    print(json.dumps(results, indent=2, ensure_ascii=True))


@main.command("metadata-put")
@click.pass_obj
@click.argument("data_path")
@click.argument("metadata_key_vals", nargs=-1, required=True)
def metadata_put(storage: Storage, data_path, metadata_key_vals):
    metadata = parse_cli_metadata(metadata_key_vals)
    storage._metadata_put(norm_data_path=data_path, metadata=metadata)


@main.command("data-exists")
@click.pass_obj
@click.argument("data_path")
def data_exists(storage: Storage, data_path: str):
    norm_data_path = storage._data_exists(norm_data_path=data_path)
    if not norm_data_path:
        raise DataDoesNotExists(data_path)
    print(norm_data_path)


@main.command("serve")
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
