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
@click.option("--json-schema", "-j")
@click.option("--table-schema", "-t")
@click.option("--json-self-validate", "-a", is_flag=True)
@click.option("--overwrite", "-w", is_flag=True)
def load(
    source,
    target,
    bytes_hash=None,
    json_schema=None,
    table_schema=None,
    json_self_validate=False,
    overwrite=False,
):
    if json_self_validate:
        if json_schema:
            raise Exception("Mutual exclusive: json-schema and json-self-validate")
        json_schema = True
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
