import logging  # noqa
from zipfile import ZipFile

from .. import utils  # noqa


def unzip_all(zipfile, target_path):
    with ZipFile(zipfile, mode="r") as zfile:
        for name in zfile.namelist():
            zfile.extract(name, path=target_path)
            yield name
