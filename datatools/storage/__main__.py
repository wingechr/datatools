"""TODO"""

import logging
import sys

import click
import uvicorn

from datatools.storage.classes import DataStorage
from datatools.storage.server import make_server_app
from datatools.utils import (
    json_dumps_for_print,
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
    ctx.obj = data_storage


@main.command()
@click.pass_obj
def info(ctx_data_storage: DataStorage) -> None:
    """TODO"""
    info = ctx_data_storage.info()
    print(json_dumps_for_print(info))


@main.command()
@click.pass_obj
@click.argument("filters", nargs=-1)
def find(ctx_data_storage: DataStorage, filters: list[str]) -> None:
    """TODO"""
    # FIXME: maybe json parse value first
    filters_dict = parse_cmd_vals(filters)
    for uid in ctx_data_storage.find(**filters_dict):
        print(uid)


@main.command()
@click.pass_obj
@click.argument("uid")
def has(ctx_data_storage: DataStorage, uid: str) -> None:
    """TODO

    just sets status code OK (0) if uid in ctx_data_storage

    """
    if uid not in ctx_data_storage:
        sys.exit(1)


@main.command()
@click.pass_obj
@click.argument("uid")
def get(ctx_data_storage: DataStorage, uid: str) -> None:
    """TODO"""
    bdata: bytes = ctx_data_storage[uid]
    sys.stdout.buffer.write(bdata)
    sys.stdout.buffer.flush()


@main.command()
@click.pass_obj
@click.argument("uid")
def put(ctx_data_storage: DataStorage, uid: str) -> None:
    """TODO"""
    # FIXME: validate uid before actually reading data
    bdata: bytes = sys.stdin.buffer.read()
    ctx_data_storage[uid] = bdata


@main.command()
@click.pass_obj
@click.argument("uid")
def delete(ctx_data_storage: DataStorage, uid: str) -> None:
    """TODO"""
    # FIXME: confirm
    del ctx_data_storage[uid]


@main.group()
@click.pass_obj
def metadata(ctx_data_storage: DataStorage) -> None:
    """TODO"""
    pass


@metadata.command("get")
@click.pass_obj
@click.argument("uid")
@click.argument("attribute")
def metadata_get(ctx_data_storage: DataStorage, uid: str, attribute: str) -> None:
    """TODO"""
    metadata_storage = ctx_data_storage.metadata(uid)
    values = list(metadata_storage[attribute])
    logging.warning("cli metadata get: %s %s", attribute, values)
    print(json_dumps_for_print(values))


@metadata.command("set")
@click.pass_obj
@click.argument("uid")
@click.argument("attribute_values", nargs=-1)
def metadata_set(
    ctx_data_storage: DataStorage, uid: str, attribute_values: list[str]
) -> None:
    """TODO"""
    metadata_storage = ctx_data_storage.metadata(uid)
    attribute_values_dct = parse_cmd_vals(attribute_values)
    # TODO: set all at once?
    logging.warning("cli metadata_set: %s", attribute_values_dct)
    for attribute, value in attribute_values_dct.items():
        metadata_storage[attribute] = value


@main.command("import")
@click.pass_obj
@click.argument("uri")
@click.argument("options", nargs=-1)
def import_from_uri(
    ctx_data_storage: DataStorage, uri: str, options: list[str]
) -> None:
    """TODO"""
    options_dict = parse_cmd_vals(options)
    uid = ctx_data_storage.import_from_uri(uri, **options_dict)
    logging.info(uid)


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


if __name__ == "__main__":
    wrap_exception(main)
