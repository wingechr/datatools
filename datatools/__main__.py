import json
import logging
import sys

import click

from datatools import Storage, __version__
from datatools.exceptions import DataDoesNotExists, DataExists
from datatools.utils import file_to_data_path


def read_uri(uri) -> bytes:
    with open(uri, "rb") as file:
        return file.read()


def write_uri(uri, data: bytes):
    raise NotImplementedError()


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
    try:
        data = storage.data_get(data_path=data_path)
    except DataDoesNotExists:
        logging.error(f"Data not found: {data_path}")
        sys.exit(1)
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
    else:
        data = read_uri(file_path)

    if not data_path and file_path:
        data_path = file_to_data_path(file_path)

    try:
        data_path = storage.data_put(
            data=data, data_path=data_path, hash_method=hash_method
        )
    except DataExists:
        logging.error(f"Data already exists: {data_path}")
        sys.exit(1)
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
@click.argument("key_values", nargs=-1, required=True)
def metadata_put(storage: Storage, data_path, key_values):
    metadata = {}
    for key_value in key_values:
        key, value = key_value.split("=")
        key = key.strip()
        value = value.strip()
        try:
            value = json.loads(value)
        except Exception:
            pass
        metadata[key] = value

    storage.metadata_put(data_path=data_path, metadata=metadata)


if __name__ == "__main__":
    main(prog_name="datatools")
