import logging  # noqa
from os.path import abspath, dirname, exists, isfile, join
from tempfile import TemporaryDirectory

from ..utils.filepath import makedirs


class FileCache:
    __slots__ = []

    def __init__(self, base_dir=None):
        if not base_dir:
            base_dir = TemporaryDirectory(prefix="datatools.cache.").name
        self.base_dir = abspath(base_dir)
        makedirs(self.base_dir)

    def _get_path_from_id(
        self,
        id,
    ):
        return id

    def _validate_path(self, path):
        path = join(self.base_dir, path)
        path = abspath(path)
        if not path.startswith(self.base_dir):
            raise Exception("invalid path")
        makedirs(dirname(path), exist_ok=True)
        return path

    def __contains__(self, id):
        path = self._get_path_from_id(id)
        path = self._validate_path(id)
        return isfile(path)

    def __get___(self, id):
        path = self._get_path_from_id(id)
        path = self._validate_path(id)

        if exists(path):
            return True
