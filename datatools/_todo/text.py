import re
from urllib.parse import unquote_plus

import unidecode


def normalize_name(name, convert_camel=True):
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

    # camel case to python
    if convert_camel:
        name = re.sub("([a-z])([A-Z])", r"\1_\2", name)

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^a-z0-9]+", "_", name)
    name = name.strip("_")
    name = re.sub("^[^a-z]*", "", name)

    return name
