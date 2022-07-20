#!/usr/bin/env python3

import logging  # noqa
import sys

import datatools
from datatools.location import location
from datatools.utils.cli import click, create_main

main = create_main(version=datatools.__version__, name="test")


@main.command()
@click.argument("source")
@click.argument("target")
@click.option("--bytes-hash", "-h")
@click.option(
    "--json-schema", "-j", help="location of schema or `auto` to load from $schema"
)
@click.option(
    "--table-schema",
    "-t",
    help="location of schema or `auto` to load from reousrce.schema",
)
@click.option("--overwrite", "-w", is_flag=True)
def load(
    source,
    target,
    bytes_hash=None,
    json_schema=None,
    table_schema=None,
    overwrite=False,
):
    if json_schema == "auto":
        json_schema = True
    if table_schema == "auto":
        table_schema = True

    source = location(source)
    target = location(target)

    if target.supports_metadata:
        metadata = {"source": str(source)}
    else:
        metadata = None

    rep = target.write(
        source.read(),
        bytes_hash=bytes_hash,
        json_schema=json_schema,
        table_schema=table_schema,
        overwrite=overwrite,
        metadata=metadata,
    )
    rep_bytes = str(rep).encode()
    sys.stdout.buffer.write(rep_bytes)


if __name__ == "__main__":
    main()
