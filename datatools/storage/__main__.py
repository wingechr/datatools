"""TODO"""

from io import BufferedReader
import logging
import sys
from typing import cast

import click
import uvicorn

from datatools.storage.base import DataStorage
from datatools.storage.http import make_server_app
from datatools.utils import (
    buffer_to_byte_iterable,
    json_dumps,
    parse_cmd_vals,
    subclasses_by_name,
    wrap_exception,
)

# we need to use print()
sys.stdout.reconfigure(errors="replace")  # type: ignore


def infer_storage_class(location: str, storage_class=str | None) -> type[DataStorage]:
    """TODO"""
    storage_classes = subclasses_by_name(DataStorage)
    if isinstance(storage_class, str) and storage_class:
        return storage_classes[storage_class]
    for cls in storage_classes.values():
        if cls._can_handle(location):
            return cls
    raise NotImplementedError(f"Cannot infer DataStorage class for location {location}")


@click.group()
@click.option("--location", "-l", default=".")
@click.option(
    "--storage_class", "-c", type=click.Choice(subclasses_by_name(DataStorage).keys())
)
@click.pass_context
def main(ctx, location: str, storage_class=str | None) -> None:
    """TODO"""
    StorageClass = infer_storage_class(location, storage_class=storage_class)
    data_storage = StorageClass(location)
    logging.debug(f"Starting {data_storage}")
    ctx.obj = data_storage


@main.command()
@click.pass_obj
def info(ctx_data_storage: DataStorage) -> None:
    """TODO"""
    info = ctx_data_storage.info()
    print(json_dumps(info))


@main.command()
@click.pass_obj
@click.argument("filters", nargs=-1)
def find(ctx_data_storage: DataStorage, filters: list[str]) -> None:
    """TODO"""
    # FIXME: maybe json parse value first
    filters_dict = parse_cmd_vals(filters)
    for name in ctx_data_storage.find(**filters_dict):
        print(name)


@main.command()
@click.pass_obj
@click.argument("name")
def has(ctx_data_storage: DataStorage, name: str) -> None:
    """TODO

    just sets status code OK (0) if name in ctx_data_storage

    """
    if not ctx_data_storage.has(name):
        sys.exit(1)


@main.command()
@click.pass_obj
@click.argument("name")
def read(ctx_data_storage: DataStorage, name: str) -> None:
    """TODO"""
    for bdata in ctx_data_storage.iter_bytes(name):
        sys.stdout.buffer.write(bdata)
    sys.stdout.buffer.flush()


@main.command()
@click.pass_obj
@click.argument("name")
def write(ctx_data_storage: DataStorage, name: str) -> None:
    """TODO"""
    # FIXME: validate name before actually reading data
    byte_iterable = buffer_to_byte_iterable(cast(BufferedReader, sys.stdin.buffer))
    ctx_data_storage.write(name, byte_iterable)


@main.command()
@click.pass_obj
@click.argument("name")
def delete(ctx_data_storage: DataStorage, name: str) -> None:
    """TODO"""
    # FIXME: confirm
    ctx_data_storage.delete(name)


@main.group()
@click.pass_obj
def metadata(ctx_data_storage: DataStorage) -> None:
    """TODO"""
    pass


@metadata.command("get")
@click.pass_obj
@click.argument("name")
@click.argument("attribute")
def metadata_get(ctx_data_storage: DataStorage, name: str, attribute: str) -> None:
    """TODO"""
    metadata_storage = ctx_data_storage.metadata(name)
    values = list(metadata_storage.get(attribute))
    print(json_dumps(values))


@metadata.command("set")
@click.pass_obj
@click.argument("name")
@click.argument("attribute_values", nargs=-1)
def metadata_set(
    ctx_data_storage: DataStorage, name: str, attribute_values: list[str]
) -> None:
    """TODO"""
    metadata_storage = ctx_data_storage.metadata(name)
    attribute_values_dct = parse_cmd_vals(attribute_values)
    for attribute, value in attribute_values_dct.items():
        metadata_storage.set(attribute, value)


@main.command("import")
@click.pass_obj
@click.argument("name")
@click.argument("uri", required=False)
@click.argument("options", nargs=-1)
def import_from_uri(
    ctx_data_storage: DataStorage, name: str, uri: str, options: list[str]
) -> None:
    """TODO"""
    options_dict = parse_cmd_vals(options)
    ctx_data_storage.import_from_uri(name, uri, **options_dict)
    logging.info(name)


@main.command("serve")
@click.pass_obj
@click.option(
    "--host",
    "-h",
    type=click.Choice(
        [
            "127.0.0.1",
            "0.0.0.0",  # noqa:S104
        ]
    ),
    default="127.0.0.1",
)
@click.option("--port", "-p", type=int, default=8000)
def serve(ctx_data_storage: DataStorage, host: str, port: int) -> None:
    """TODO"""
    app = make_server_app(data_storage=ctx_data_storage)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":  # pragma: no cover
    wrap_exception(main)
