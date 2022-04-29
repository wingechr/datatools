import logging
import traceback
from logging import DEBUG, ERROR, INFO, WARNING  # noqa


def get_level(level=None):
    level = level or logging.WARNING
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    return level


def basicConfig(level=None):
    logging.basicConfig(
        format="[%(asctime)s %(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=get_level(level),
    )


def show_trace(func):
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            raise

    return decorator
