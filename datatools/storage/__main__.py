"""TODO"""

from io import BufferedReader
import logging
import sys
from typing import cast

import click
import uvicorn

from datatools.io import JsonIO
from datatools.storage import storage, storage_classes
from datatools.storage.base import DataStorage
from datatools.storage.http import make_server_app
from datatools.storage.mail import (
    DEFAULT_IMAP_FOLDER,
    DEFAULT_IMAP_PORT,
    MailAttachmentStorageHandler,
)
from datatools.utils import (
    buffer_to_byte_iterable,
    parse_cmd_vals,
    wrap_exception,
)

# we need to use print()
sys.stdout.reconfigure(errors="replace")  # type:ignore reconfigure does exist


@click.group()
@click.option("--location", "-l", default=".")
@click.option("--storage_class", "-c", type=click.Choice(storage_classes.keys()))
@click.pass_context
def main(ctx, location: str, storage_class=str | None) -> None:
    """TODO"""
    ctx.obj = storage(location, storage_class=storage_class)
    logging.debug(f"Starting {ctx.obj}")


@main.command()
@click.pass_obj
def info(ctx_data_storage: DataStorage) -> None:
    """TODO"""
    info = ctx_data_storage.info()
    print(JsonIO.dumps(info))


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
    print(JsonIO.dumps(values))


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
@click.option("--skip-finished", "-s", is_flag=True)
@click.argument("options", nargs=-1)
def import_from_uri(
    ctx_data_storage: DataStorage,
    name: str,
    uri: str,
    skip_finished: bool,
    options: list[str],
) -> None:
    """TODO"""
    options_dict = parse_cmd_vals(options)
    ctx_data_storage.import_from_uri(
        uri=uri, name=name, skip_finished=skip_finished, **options_dict
    )
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


@main.command("monitor-mailbox")
@click.pass_obj
@click.argument("login_mail")
@click.option("--imap-port", "-p", type=int, default=DEFAULT_IMAP_PORT)
@click.option("--imap-folder", "-f", type=str, default=DEFAULT_IMAP_FOLDER)
@click.option("--email-whitelist", "-w", type=str, multiple=True)
def monitor_mailbox(  # pragma: no cover - we test the imap monitor directly
    ctx_data_storage: DataStorage,
    login_mail: str,
    imap_port: int,
    imap_folder: str,
    email_whitelist: list[str],
) -> None:
    """TODO"""
    monitor = MailAttachmentStorageHandler(
        storage=ctx_data_storage,
        login_mail=login_mail,
        email_whitelist=email_whitelist,
        imap_port=imap_port,
        imap_folder=imap_folder,
    )
    monitor.serve_forever()


if __name__ == "__main__":  # pragma: no cover
    wrap_exception(main)
