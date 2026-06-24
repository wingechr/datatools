"""TODO"""

import click

from datatools.utils import wrap_exception


@click.group()
@click.pass_context
def main(ctx) -> None:
    """TODO"""
    ctx.obj = None


if __name__ == "__main__":
    wrap_exception(main)
