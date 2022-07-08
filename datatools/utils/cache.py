import logging  # noqa
import os
import tempfile

from .. import utils  # noqa
from ..utils.filepath import make_file_readlonly, makedirs, move


class Cache:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self._init()

    def _init(self):
        makedirs(self.base_dir)

    def _get_path(
        self,
        id,
    ):
        path = os.path.join(self.base_dir, id)
        path = os.path.abspath(path)
        if not path.startswith(self.base_dir):
            raise Exception("invalid path")

        return path

    def __contains__(self, id):
        path = self._get_path(id)
        if os.path.exists(path):
            return False
        if os.path.isfile(path):
            return path
        raise Exception("not a normal file: %s" % path)

    def __get___(self, id):
        path = self.__contains__(id)
        if os.path.exists(path):
            return True


class FileSystemStorage:

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None, sf_depth=0, sf_len=2):
        self.data_dir = os.path.abspath(data_dir or self.DEFAULT_DATA_DIR)
        self.sf_depth = sf_depth
        self.sf_len = sf_len

    def __contains__(self, file_id):
        file_path = self._get_file_path(file_id)
        return os.path.isfile(file_path)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _get_file_path(self, file_id):
        data_dir = self.data_dir
        for i in range(self.sf_depth):
            subfolder = file_id[i * self.sf_len : (i + 1) * self.sf_len]
            data_dir = os.path.join(data_dir, subfolder)
        if not os.path.isdir(data_dir):
            logging.debug("creating data dir: %s", data_dir)
            makedirs(data_dir)
        return os.path.join(data_dir, file_id)

    def get_file(self, file_id, check_integrity=False):
        """
        Args:
            file_id(str): 64 character sha256 hash
            check_integrity(bool): check hash on read

        Returns:
            data_stream (IOBase like)

        Raises:
            ObjectNotFoundException
        """
        if file_id not in self:
            raise Exception(file_id)
        file_path = self._get_file_path(file_id)
        file = open(file_path, "rb").__enter__()
        if check_integrity:
            file = utils.byte.HashedIterator(file, expected_hash=file_id)
        return file

    def set_file(self, data_stream):
        """
        Args:
            data_stream: IOBase like readable binary stream

        Returns:
            file_id(str): 64 character sha256 hash
        """
        data_stream = utils.byte.HashedIterator(data_stream)
        with tempfile.NamedTemporaryFile("wb", delete=False) as file:
            for chunk in data_stream:
                file.write(chunk)
        file_id = data_stream.get_current_hash()
        file_size = data_stream.get_current_size_bytes()
        file_path = self._get_file_path(file_id)
        tmp_file_path = file.name
        if os.path.isfile(file_path):
            # file exists already
            logging.debug("file already in storage: %s (%d bytes)", file_id, file_size)
            os.remove(tmp_file_path)
        else:
            # copy file
            logging.debug(
                "adding file %s: %s (%d bytes)", file_path, file_id, file_size
            )
            move(tmp_file_path, file_path)
            # make readonly
            make_file_readlonly(file_path)
        return file_id
