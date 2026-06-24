"""TODO"""

import logging
import sys

import click
import uvicorn

from datatools.storage.server import make_server_app
from datatools.storage.types import DataStorage
from datatools.utils import iter_subclasses, parse_cmd_vals, wrap_exception

# we need to use print()
sys.stdout.reconfigure(errors="replace")  # type: ignore

REGISTERES_STORAGE_CLASSES = {
    c.__name__: c for c in list(iter_subclasses(DataStorage))[1:]
}


def infer_storage_class(location: str, storage_class=str | None) -> type[DataStorage]:
    """TODO"""
    if isinstance(storage_class, str) and storage_class:
        return REGISTERES_STORAGE_CLASSES[storage_class]
    for cls in REGISTERES_STORAGE_CLASSES.values():
        if cls._can_handle_location(location):
            return cls
    raise Exception(f"Cannot infer DataStorage class for location {location}")


@click.group()
@click.option("--location", "-l", default=".")
@click.option(
    "--storage_class", "-c", type=click.Choice(REGISTERES_STORAGE_CLASSES.keys())
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
    for k, v in ctx_data_storage.info().items():
        logging.info("%s: %s", k, v)


@main.command()
@click.pass_obj
@click.argument("filters", nargs=-1)
def find(ctx_data_storage: DataStorage, filters: list[str]) -> None:
    """TODO"""
    # FIXME: maybe json parse value first
    filters_dict = parse_cmd_vals(filters)
    for uid in ctx_data_storage.list(**filters_dict):
        print(uid)


@main.command()
@click.pass_obj
@click.argument("uid")
def has(ctx_data_storage: DataStorage, uid: str) -> None:
    """TODO

    just sets status code

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
    values = metadata_storage[attribute]
    for value in values:
        # FIXME: maybe json dumps value first?
        print(value)


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
    for attribute, value in attribute_values_dct.items():
        metadata_storage[attribute] = value


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
