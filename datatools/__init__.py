"""data processing tools."""

__version__ = "0.14.0"
__all__ = ["FileDataStorage"]

# import classes so they re registrerd (TODO)
from . import importer  # noqa
from .storage import classes  # noqa


from datatools.storage.classes import FileDataStorage
