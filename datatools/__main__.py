"""datatools cmd entry point."""

import logging

import click
import coloredlogs

from datatools.job.__main__ import main as job_main
from datatools.storage.__main__ import main as storage_main
from datatools.utils import wrap_exception

LOGLEVELS_COLORS = {
    "debug": "blue",
    "info": "green",
    "warning": "yellow",
    "error": "red",
}


@click.group()
@click.option(
    "--loglevel", "-l", type=click.Choice(LOGLEVELS_COLORS.keys()), default="info"
)
def main(loglevel: str) -> None:
    """TODO"""
    coloredlogs.install(
        level=getattr(logging, loglevel.upper()),
        fmt="[%(asctime)s %(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        field_styles={k: {"color": "white"} for k in ["asctime", "levelname"]},
        level_styles={k: {"color": v} for k, v in LOGLEVELS_COLORS.items()},
    )
    logging.debug("loglevel: %s", loglevel)


main.add_command(storage_main, name="storage")
main.add_command(job_main, name="job")

if __name__ == "__main__":
    wrap_exception(main)
