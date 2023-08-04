# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import json
import logging
import os
import re
from io import BufferedReader
from typing import Iterable, Union

import jsonpath_ng

from .constants import (
    DATA_PATH_PREFIX,
    DEFAULT_HASH_METHOD,
    HASHED_DATA_PATH_PREFIX,
    LOCAL_LOCATION,
    ROOT_METADATA_PATH,
)
from .exceptions import DataDoesNotExists, DataExists, InvalidPath
from .utils import (
    HashedNamedTemporaryFile,
    NewNamedTemporaryFile,
    as_byte_iterator,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)


class Storage:
    def __init__(self, location=None):
        self.location = os.path.abspath(location or LOCAL_LOCATION)

    def __str__(self):
        return f"Storage({self.location})"

    def _get_norm_data_path(self, data_path: str) -> str:
        """should be all lowercase
        Returns:
            str: norm_data_path
        Raises:
            InvalidPath
        """
        # TODO: better solution
        if data_path.startswith(DATA_PATH_PREFIX + "/"):
            n_prefix = len(DATA_PATH_PREFIX + "/")
            data_path = data_path[n_prefix:]

        norm_data_path = DATA_PATH_PREFIX + "/" + normalize_path(data_path)

        # TODO: create function is_metadata_path
        if re.match(r".*\.metadata\..*", norm_data_path):
            raise InvalidPath(data_path)

        return norm_data_path

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern

    def _get_data_filepath(self, norm_data_path: str) -> str:
        # remote DATA_PATH_PREFIX from norm_data_path
        n_prefix = len(DATA_PATH_PREFIX) + 1  # + 1 for slash
        data_path = norm_data_path[n_prefix:]
        data_filepath = os.path.join(self.location, data_path)
        data_filepath = os.path.abspath(data_filepath)
        return data_filepath

    def _get_metadata_filepath(self, norm_data_path: str) -> str:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        metadata_filepath = os.path.abspath(metadata_filepath)
        return metadata_filepath

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        norm_data_path = self._get_norm_data_path(data_path)
        metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
        metadata_filepath = self._get_metadata_filepath(norm_data_path=norm_data_path)
        if not os.path.exists(metadata_filepath):
            return None
        logging.debug(f"READING {metadata_filepath}")
        with open(metadata_filepath, "rb") as file:
            metadata = json.load(file)
        match = metadata_path_pattern.find(metadata)
        result = [x.value for x in match]
        # TODO: we always get a list (multiple matches),
        # but most of the time, we want only one
        if len(result) == 1:
            result = result[0]
        logging.debug(f"get metadata: {metadata_path} => {result}")
        return result

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        norm_data_path = self._get_norm_data_path(data_path)
        metadata_filepath = self._get_metadata_filepath(norm_data_path=norm_data_path)
        os.makedirs(os.path.dirname(metadata_filepath), exist_ok=True)

        if not os.path.exists(metadata_filepath):
            _metadata = {}
        else:
            logging.debug(f"READING {metadata_filepath}")
            with open(metadata_filepath, "rb") as file:
                _metadata = json.load(file)

        for metadata_path, value in metadata.items():
            metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
            logging.debug(f"update metadata: {metadata_path} => {value}")
            metadata_path_pattern.update_or_create(_metadata, value)

        metadata_bytes = json.dumps(
            _metadata, indent=2, ensure_ascii=False, default=json_serialize
        ).encode()

        logging.debug(f"WRITING {metadata_filepath}")
        with open(metadata_filepath, "wb") as file:
            file.write(metadata_bytes)

    def data_post(
        self,
        data: Union[BufferedReader, bytes, Iterable],
    ) -> str:
        hash_method = DEFAULT_HASH_METHOD
        dummy_filepath = os.path.join(
            self.location, HASHED_DATA_PATH_PREFIX, hash_method
        )

        with HashedNamedTemporaryFile(dummy_filepath) as file:
            logging.debug(f"WRITING {file.filepath_temp}")
            for chunk in as_byte_iterator(data):
                file.write(chunk)

        norm_data_path = (
            f"{DATA_PATH_PREFIX}/{HASHED_DATA_PATH_PREFIX}/{hash_method}/{file.hashsum}"
        )
        metadata = file.metadata
        self.metadata_put(data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def data_put(
        self,
        data: Union[BufferedReader, bytes, Iterable],
        data_path: str,
        exist_ok=False,
    ) -> str:
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        if self.data_exists(data_path=norm_data_path):
            if not exist_ok:
                raise DataExists(norm_data_path)
            logging.info(f"Skipping existing file: {norm_data_path}")
            return norm_data_path

        # TODO replace startswith with function
        if norm_data_path.startswith(f"{DATA_PATH_PREFIX}/{HASHED_DATA_PATH_PREFIX}/"):
            raise InvalidPath(norm_data_path)

        # write data
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        with NewNamedTemporaryFile(filepath=data_filepath) as file:
            logging.debug(f"WRITING {file.filepath_temp}")
            for chunk in as_byte_iterator(data):
                file.write(chunk)

        metadata = file.metadata
        metadata["source.name"] = norm_data_path
        self.metadata_put(data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def data_open(self, data_path: str) -> BufferedReader:
        data_filepath = self.get_filepath(data_path=data_path)
        logging.debug(f"READING {data_filepath}")
        file = open(data_filepath, "rb")
        return file

    def get_filepath(self, data_path: str) -> str:
        norm_data_path = self.data_exists(data_path=data_path)
        if not norm_data_path:
            raise DataDoesNotExists(data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        # just to be sure
        make_file_readonly(data_filepath)
        return data_filepath

    def data_exists(self, data_path) -> str:
        norm_data_path = self._get_norm_data_path(data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            return norm_data_path

    def data_delete(self, data_path: str) -> None:
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        # if path does not exist: ignore
        if os.path.exists(data_filepath):
            make_file_writable(file_path=data_filepath)
            logging.debug(f"DELETING {data_filepath}")
            os.remove(data_filepath)
