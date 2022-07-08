import logging  # noqa
import re
from urllib.parse import unquote_plus

import inflection
import unidecode

from .. import utils  # noqa


def normalize(name):
    """
    >>> normalize_name('Hello  World!')
    'hello_world'
    >>> normalize_name('helloWorld')
    'hello_world'
    >>> normalize_name('_private_4')
    '_private_4'
    >>> normalize_name('François fährt Straßenbahn zum Café Málaga')
    'francois_faehrt_strassenbahn_zum_cafe_malaga'
    """

    name = unquote_plus(name)

    # manual replacements for german
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("Ä", "Ae"),
        ("Ö", "Oe"),
        ("Ü", "Ue"),
        ("ß", "ss"),
    ]:
        name = name.replace(cin, cout)

    # maske ascii
    name = unidecode.unidecode(name)

    name = inflection.dasherize(name)

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^a-z0-9]+", "_", name)
    name = name.strip("_")
    name = re.sub("^[^a-z]*", "", name)

    return name
