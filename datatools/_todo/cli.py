#!/usr/bin/env python3

import logging
import sys

import click
import coloredlogs

from datatools.combined import CombinedLocalStorage
from datatools.utils import json_dumps

__version__ = "0.0.0"

# https://coloredlogs.readthedocs.io/en/latest/api.html#changing-the-date-time-format
coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
coloredlogs.DEFAULT_FIELD_STYLES = {
    "asctime": {"color": "black", "bold": True},  # gray
    "levelname": {"color": "black", "bold": True},  # gray
}
coloredlogs.DEFAULT_LEVEL_STYLES = {
    "debug": {"color": "black", "bold": True},  # gray
    "info": {"color": "white"},
    "warning": {"color": "yellow"},
    "error": {"color": "red", "bold": 10},
}


@click.group()
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"]),
    default="info",
)
def main(ctx, loglevel):
    """Script entry point."""
    if isinstance(loglevel, str):
        loglevel = getattr(logging, loglevel.upper())
    coloredlogs.install(level=loglevel)
    ctx.ensure_object(dict)


@main.group("file")
@click.pass_context
@click.option("--data-dir", "-d")
def file(ctx, data_dir):
    ctx.obj["data_dir"] = data_dir


@file.command("set")
@click.pass_context
@click.option("--filepath", "-f", type=click.Path(exists=True))
def file_set(ctx, filepath):
    data_dir = ctx.obj["data_dir"]
    with CombinedLocalStorage(data_dir=data_dir) as storage:
        logging.debug(filepath)
        if filepath:
            file_id = storage.set_file_by_path(filepath)
        else:
            file = sys.stdin.buffer
            file_id = storage.set_file(file)
    print(file_id)


@file.command("get")
@click.pass_context
@click.argument("file_id")
@click.option("--filepath", "-f", type=click.Path(exists=False))
@click.option("--check-integrity", "-c", is_flag=True)
def file_get(ctx, file_id, filepath, check_integrity):
    data_dir = ctx.obj["data_dir"]
    with CombinedLocalStorage(data_dir=data_dir) as storage:
        if file_id not in storage:
            logging.error("File not found")
            click.Abort()
            sys.exit(1)
        if filepath:
            with open(filepath, "wb") as file:
                for chunk in storage.get_file(file_id, check_integrity=check_integrity):
                    file.write(chunk)
        else:
            file = sys.stdout.buffer
            for chunk in storage.get_file(file_id, check_integrity=check_integrity):
                file.write(chunk)
                file.flush()


@main.group("metadata")
@click.pass_context
@click.option("--data-dir", "-d")
def metadata(ctx, data_dir):
    ctx.obj["data_dir"] = data_dir


@metadata.command("get-all")
@click.pass_context
@click.argument("file_id")
@click.option("--extended", "-e", is_flag=True)
def metadata_get_all(ctx, file_id, extended):
    data_dir = ctx.obj["data_dir"]
    with CombinedLocalStorage(data_dir=data_dir) as storage:
        if extended:
            metadata = storage.get_all_metadata_extended(file_id)
            for m in metadata:
                m["value"] = json_dumps(m["value"])
                print("%(identifier)s = %(value)s [%(user)s %(timestamp_utc)s]" % m)
        else:
            metadata = storage.get_all_metadata(file_id)
            for k, v in metadata.items():
                print("%s = %s" % (k, json_dumps(v)))


if __name__ == "__main__":
    main(prog_name="datatools")
