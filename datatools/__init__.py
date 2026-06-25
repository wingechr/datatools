"""data processing tools."""

__version__ = "0.14.0"
__all__ = []

# import classes so they re registrerd (TODO)
from . import importer  # noqa
from .storage import classes  # noqa


def self_check():
    """check that setup is working.

    Example:

        >>> self_check()
        True

    """
    return True


# self check on import
self_check()
