import logging  # noqa
from os.path import abspath, dirname, isfile, join
from tempfile import mkdtemp

from genericpath import isdir

from ..utils.byte import Iterator
from ..utils.filepath import make_file_readlonly, makedirs

# from ..utils.filepath import make_file_writable


class FileCache:
    __slots__ = ["base_dir"]

    hash_method = "sha256"

    def __init__(self, base_dir=None):
        if not base_dir:
            base_dir = mkdtemp(prefix="datatools.cache.")
        self.base_dir = abspath(base_dir)
        # logging.debug("cache dir: %s", self.base_dir)
        makedirs(self.base_dir)

    def _get_path_from_id(
        self,
        id,
    ):
        return str(id)

    def _get_path_exists(
        self,
        id,
    ):
        path = self._get_path_from_id(id)
        path = self._validate_path(path)
        return path, isfile(path)

    def _validate_path(self, path):
        path = join(self.base_dir, path)
        path = abspath(path)
        if not path.startswith(self.base_dir):
            raise Exception("invalid path")
        if isdir(path):
            raise Exception("invalid path")
        makedirs(dirname(path), exist_ok=True)
        return path

    def __contains__(self, id):
        _, exists = self._get_path_exists(id)
        return exists

    def __getitem__(self, id):
        path, exists = self._get_path_exists(id)
        if not exists:
            raise KeyError(id)
        return Iterator(path)

    def __setitem__(self, id, data):
        path, exists = self._get_path_exists(id)

        it = Iterator(data, hash_method=self.hash_method)
        byte_data = it.read()
        if exists:
            with Iterator(path, hash_method=self.hash_method) as it_old:
                it_old.read()
                if it_old.get_current_hash() != it.get_current_hash():
                    raise Exception("hash changed")

            # make_file_writable(path)
            return  # do not overwrite

        with open(path, "wb") as file:
            file.write(byte_data)
        make_file_readlonly(path)
