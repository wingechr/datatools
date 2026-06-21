"""TODO"""

import logging
import sys

import click

from ..utils import wrap_exception
from .classes import FileDataStorage
from .types import DataStorage, MetadataStorage

# we need to use print()
sys.stdout.reconfigure(errors="replace")  # type: ignore


def get_storage_class_from_location(location: str) -> type[DataStorage]:
    """TODO"""
    # FIXME
    return FileDataStorage


@click.group()
@click.argument("location")
@click.pass_context
def main(ctx, location: str) -> None:
    """TODO"""
    StorageClass = get_storage_class_from_location(location)
    data_storage = StorageClass(location)
    ctx.ensure_object(dict)
    ctx.obj["data_storage"] = data_storage


@main.command()
@click.pass_context
def info(ctx) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    logging.info("Class: %s", data_storage.__class__.__name__)
    logging.info("Location: %s", data_storage._location)


def parse_cmd_vals(arguments: list[str]) -> dict[str, str]:
    """TODO"""
    return dict(kv.split("=", 1) for kv in arguments)


@main.command()
@click.pass_context
@click.argument("filters", nargs=-1)
def find(ctx, filters: list[str]) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    # FIXME: maybe json parse value first
    filters_dict = parse_cmd_vals(filters)
    for uid in data_storage.list(**filters_dict):
        print(uid)


@main.command()
@click.argument("uid")
@click.pass_context
def get(ctx, uid: str) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    bdata: bytes = data_storage[uid]
    sys.stdout.buffer.write(bdata)
    sys.stdout.buffer.flush()


@main.command()
@click.argument("uid")
@click.pass_context
def put(ctx, uid: str) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    # FIXME: validate uid before actually reading data
    bdata: bytes = sys.stdin.buffer.read()
    data_storage[uid] = bdata


@main.command()
@click.argument("uid")
@click.pass_context
def delete(ctx, uid: str) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    # FIXME: confirm
    del data_storage[uid]


@main.group()
@click.argument("uid")
@click.pass_context
def metadata(ctx, uid: str) -> None:
    """TODO"""
    data_storage: DataStorage = ctx.obj["data_storage"]
    ctx.obj["metadata_storage"] = data_storage.metadata(uid)


@metadata.command("get")
@click.argument("attribute")
@click.pass_context
def metadata_get(ctx, attribute: str) -> None:
    """TODO"""
    metadata_storage: MetadataStorage = ctx.obj["metadata_storage"]
    with metadata_storage:
        values = metadata_storage[attribute]
        for value in values:
            # FIXME: maybe json dumps value first?
            print(value)


@metadata.command("set")
@click.pass_context
@click.argument("attribute_values", nargs=-1)
def metadata_set(ctx, attribute_values: list[str]) -> None:
    """TODO"""
    metadata_storage: MetadataStorage = ctx.obj["metadata_storage"]
    # FIXME: maybe json parse value first
    attribute_values_dct = parse_cmd_vals(attribute_values)
    with metadata_storage:
        for attribute, value in attribute_values_dct.items():
            metadata_storage[attribute] = value


if __name__ == "__main__":
    wrap_exception(main)
