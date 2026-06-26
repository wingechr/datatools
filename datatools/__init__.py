"""data processing tools."""

__version__ = "0.14.0"
__all__ = []

# import classes so they re registrerd (TODO)
from . import importer  # noqa
from .storage import classes  # noqa
import warnings


# hide rdflib\graph.py: DeprecationWarning: Dataset.default_context is deprecated
warnings.filterwarnings("ignore", category=DeprecationWarning, module="rdflib.*")


def self_check():
    """check that setup is working.

    Example:

        >>> self_check()
        True

    """
    return True


# self check on import
self_check()
