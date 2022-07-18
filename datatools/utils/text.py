import logging  # noqa
import re
from urllib.parse import unquote_plus

import inflection
import unidecode


def normalize(name, allowed_chars="a-z0-9", sep="_"):

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
    name = re.sub("[^" + allowed_chars + "]+", sep, name)
    name = name.rstrip(sep)

    return name
