"""TODO"""

import logging
import sys

import click
from pydantic import BaseModel, ConfigDict
import uvicorn

from datatools.storage.server import make_server_app
from datatools.storage.types import DataStorage, MetadataStorage
from datatools.utils import iter_subclasses, parse_cmd_vals, wrap_exception


class ClickContextObject(BaseModel):
    """TODO"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    data_storage: DataStorage
    metadata_storage: MetadataStorage | None = None


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
    ctx.obj = ClickContextObject(data_storage=data_storage)


@main.command()
@click.pass_obj
def info(ctx_obj: ClickContextObject) -> None:
    """TODO"""
    for k, v in ctx_obj.data_storage.info().items():
        logging.info("%s: %s", k, v)


@main.command()
@click.pass_obj
@click.argument("filters", nargs=-1)
def find(ctx_obj: ClickContextObject, filters: list[str]) -> None:
    """TODO"""
    # FIXME: maybe json parse value first
    filters_dict = parse_cmd_vals(filters)
    for uid in ctx_obj.data_storage.list(**filters_dict):
        print(uid)


@main.command()
@click.argument("uid")
@click.pass_obj
def has(ctx_obj: ClickContextObject, uid: str) -> None:
    """TODO

    just sets status code

    """
    if uid not in ctx_obj.data_storage:
        sys.exit(1)


@main.command()
@click.argument("uid")
@click.pass_obj
def get(ctx_obj: ClickContextObject, uid: str) -> None:
    """TODO"""
    bdata: bytes = ctx_obj.data_storage[uid]
    sys.stdout.buffer.write(bdata)
    sys.stdout.buffer.flush()


@main.command()
@click.argument("uid")
@click.pass_obj
def put(ctx_obj: ClickContextObject, uid: str) -> None:
    """TODO"""
    # FIXME: validate uid before actually reading data
    bdata: bytes = sys.stdin.buffer.read()
    ctx_obj.data_storage[uid] = bdata


@main.command()
@click.argument("uid")
@click.pass_obj
def delete(ctx_obj: ClickContextObject, uid: str) -> None:
    """TODO"""
    # FIXME: confirm
    del ctx_obj.data_storage[uid]


@main.group()
@click.argument("uid")
@click.pass_obj
def metadata(ctx_obj: ClickContextObject, uid: str) -> None:
    """TODO"""
    ctx_obj.metadata_storage = ctx_obj.data_storage.metadata(uid)


@metadata.command("get")
@click.argument("attribute")
@click.pass_obj
def metadata_get(ctx_obj: ClickContextObject, attribute: str) -> None:
    """TODO"""
    assert ctx_obj.metadata_storage  # noqa
    values = ctx_obj.metadata_storage[attribute]
    for value in values:
        # FIXME: maybe json dumps value first?
        print(value)


@metadata.command("set")
@click.pass_obj
@click.argument("attribute_values", nargs=-1)
def metadata_set(ctx_obj: ClickContextObject, attribute_values: list[str]) -> None:
    """TODO"""
    assert ctx_obj.metadata_storage  # noqa
    # FIXME: maybe json parse value first
    attribute_values_dct = parse_cmd_vals(attribute_values)
    for attribute, value in attribute_values_dct.items():
        ctx_obj.metadata_storage[attribute] = value


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
def serve(ctx_obj: ClickContextObject, host: str, port: int) -> None:
    """TODO"""
    app = make_server_app(data_storage=ctx_obj.data_storage)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    wrap_exception(main)
