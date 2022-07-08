import logging  # noqa
import re
from urllib.parse import unquote_plus

import inflection
import unidecode


def normalize(name):

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

    name = inflection.underscore(name)

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^a-z0-9]+", "_", name)
    name = name.rstrip("_")

    return name
