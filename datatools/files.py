import logging
import os
import shutil
import tempfile

from .exceptions import ObjectNotFoundException
from .utils import HashedByteIterator, make_file_readlonly, validate_file_id


class FileSystemStorage:

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None, sf_depth=0, sf_len=2):
        self.data_dir = os.path.abspath(data_dir or self.DEFAULT_DATA_DIR)
        self.sf_depth = sf_depth
        self.sf_len = sf_len

    def __contains__(self, file_id):
        filepath = self._get_filepath(file_id)
        return os.path.isfile(filepath)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _get_filepath(self, file_id):
        data_dir = self.data_dir
        for i in range(self.sf_depth):
            subfolder = file_id[i * self.sf_len : (i + 1) * self.sf_len]
            data_dir = os.path.join(data_dir, subfolder)
        if not os.path.isdir(data_dir):
            logging.debug("creating data dir: %s", data_dir)
            os.makedirs(data_dir)
        return os.path.join(data_dir, file_id)

    def get_file(self, file_id, check_integrity=False):
        """
        Args:
            file_id(str): 32 character md5 hash
            check_integrity(bool): check hash on read

        Returns:
            data_stream (IOBase like)

        Raises:
            ObjectNotFoundException
        """
        file_id = validate_file_id(file_id)
        if file_id not in self:
            raise ObjectNotFoundException(file_id)
        filepath = self._get_filepath(file_id)
        file = open(filepath, "rb").__enter__()
        if check_integrity:
            file = HashedByteIterator(file, expected_hash=file_id)
        return file

    def set_file(self, data_stream):
        """
        Args:
            data_stream: IOBase like readable binary stream

        Returns:
            file_id(str): 32 character md5 hash
        """
        data_stream = HashedByteIterator(data_stream)
        with tempfile.NamedTemporaryFile("wb", delete=False) as file:
            for chunk in data_stream:
                file.write(chunk)
        file_id = data_stream.get_current_hash()
        file_size = data_stream.get_current_size_bytes()
        filepath = self._get_filepath(file_id)
        tmp_filepath = file.name
        if os.path.isfile(filepath):
            # file exists already
            logging.debug("file already in storage: %s (%d bytes)", file_id, file_size)
            os.remove(tmp_filepath)
        else:
            # copy file
            logging.debug("adding file %s: %s (%d bytes)", filepath, file_id, file_size)
            shutil.move(tmp_filepath, filepath)
            # make readonly
            make_file_readlonly(filepath)
        return file_id
