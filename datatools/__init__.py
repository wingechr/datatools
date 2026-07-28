"""data processing tools."""

__version__ = "0.16.1"
__all__ = ["FileDataStorage", "storage", "AnnotatedFunction", "Resource"]

from datatools.process.task import AnnotatedFunction
from datatools.storage import FileDataStorage, Resource, storage
