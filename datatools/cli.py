#!/usr/bin/env python3

import logging
import sys

import click
import coloredlogs


from .storage.combined import CombinedLocalStorage

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


@main.group()
@click.pass_context
@click.option("--data-dir", "-d")
def file(ctx, data_dir):
    ctx.obj["data_dir"] = data_dir


@file.command("set")
@click.pass_context
@click.option("--filepath", "-f", type=click.Path(exists=True))
def file_set(ctx, filepath):
    data_dir = ctx.obj["data_dir"]
    with CombinedLocalStorage(data_dir=data_dir) as fss:
        if filepath:
            with open(filepath, "rb") as file:
                file_id = fss.files.set(file)
        else:
            file = sys.stdin.buffer
            file_id = fss.files.set(file)
    print(file_id)


@file.command("get")
@click.pass_context
@click.argument("file_id")
@click.option("--filepath", "-f", type=click.Path(exists=False))
@click.option("--check-integrity", "-c", is_flag=True)
def file_set(ctx, file_id, filepath, check_integrity):
    data_dir = ctx.obj["data_dir"]
    with CombinedLocalStorage(data_dir=data_dir) as fss:
        if file_id not in fss.files:
            logging.error("File not found")
            click.Abort()
            sys.exit(1)
        if filepath:
            with open(filepath, "wb") as file:
                for chunk in fss.files.get(file_id, check_integrity=check_integrity):
                    file.write(chunk)
        else:
            file = sys.stdout.buffer
            for chunk in fss.files.get(file_id, check_integrity=check_integrity):
                file.write(chunk)


if __name__ == "__main__":
    main(prog_name="datatools")
