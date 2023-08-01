# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import hashlib
import json
import logging
import os
import re
from io import BufferedReader
from tempfile import NamedTemporaryFile
from typing import Iterable, Union

import jsonpath_ng

from .cache import cache
from .exceptions import DataDoesNotExists, DataExists, IntegrityError, InvalidPath
from .load import open_uri
from .utils import (
    as_byte_iterator,
    as_uri,
    get_default_storage_location,
    get_now_str,
    get_user_w_host,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)

# remote
ROOT_METADATA_PATH = "$"  # root
HASHED_DATA_PATH_PREFIX = "hash/"
ALLOWED_HASH_METHODS = ["md5", "sha256"]

DEFAULT_HASH_METHOD = ALLOWED_HASH_METHODS[0]

GLOBAL_LOCATION = get_default_storage_location()
LOCAL_LOCATION = "__data__"


class Storage:
    def __init__(self, location=None):
        location = location or LOCAL_LOCATION
        self.location = os.path.abspath(location)
        logging.debug(f"Location: {self.location}")

    def __enter__(self):
        # does not do anything currently
        return self

    def __exit__(self, *args):
        pass

    def __str__(self):
        return f"Storage({self.location})"

    def _get_norm_data_path(self, data_path: str) -> str:
        """should be all lowercase
        Returns:
            str: norm_data_path
        Raises:
            InvalidPath
        """
        norm_data_path = normalize_path(data_path)
        # TODO: maybe later: remove double check:
        if norm_data_path != normalize_path(norm_data_path):
            raise InvalidPath(data_path)
        # TODO: create function is_metadata_path
        if re.match(r".*\.metadata\..*", norm_data_path):
            raise InvalidPath(data_path)
        return norm_data_path

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern

    def _get_data_filepath(self, norm_data_path: str, create_parent_dir=False) -> str:
        data_filepath = os.path.join(self.location, norm_data_path)
        data_filepath = self._get_abs_path_with_parent(
            path=data_filepath, create_parent_dir=create_parent_dir
        )
        return data_filepath

    def _get_metadata_filepath(
        self, norm_data_path: str, create_parent_dir=False
    ) -> str:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        metadata_filepath = self._get_abs_path_with_parent(
            path=metadata_filepath, create_parent_dir=create_parent_dir
        )
        return metadata_filepath

    def _get_abs_path_with_parent(self, path, create_parent_dir) -> str:
        path = os.path.abspath(path)
        if create_parent_dir:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

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
        metadata_filepath = self._get_metadata_filepath(
            norm_data_path=norm_data_path, create_parent_dir=True
        )

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

    def data_put(
        self,
        data: Union[BufferedReader, bytes, Iterable],
        data_path: str = None,
        exist_ok=None,
    ) -> str:
        # if not datapath: map to default hash endpoint
        if not data_path:
            norm_data_path = self._get_norm_data_path(
                f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}"
            )
            if exist_ok is False:
                logging.warning("without a data path, exist_ok = False will be ignored")
            exist_ok = True
        else:
            exist_ok = bool(exist_ok)
            norm_data_path = self._get_norm_data_path(data_path)

        filepath_is_hash = norm_data_path.startswith(HASHED_DATA_PATH_PREFIX)

        if filepath_is_hash:
            offset = len(HASHED_DATA_PATH_PREFIX)
            hash_method = norm_data_path[offset:]
            if hash_method not in ALLOWED_HASH_METHODS:
                raise InvalidPath(norm_data_path)
            # add a placeholder for filename
            norm_data_path = norm_data_path + "/__HASHFILE__"
        else:
            hash_method = DEFAULT_HASH_METHOD

        if not filepath_is_hash and self.data_exists(data_path=norm_data_path):
            if not exist_ok:
                raise DataExists(norm_data_path)
            logging.info(f"Skipping existing file: {norm_data_path}")
            return norm_data_path

        # write data
        data_filepath = self._get_data_filepath(
            norm_data_path=norm_data_path, create_parent_dir=True
        )
        data_dir = os.path.dirname(data_filepath)
        filename = os.path.basename(data_filepath)

        size = 0
        hasher = getattr(hashlib, hash_method)()
        data = as_byte_iterator(data)

        with NamedTemporaryFile(
            mode="wb", dir=data_dir, delete=False, prefix=filename + "+"
        ) as file:
            try:
                logging.debug(f"WRITING {file.name}")
                for chunk in data:
                    size += len(chunk)
                    hasher.update(chunk)
                    file.write(chunk)
            except Exception:
                file.close()
                os.remove(file.name)
                raise

        hashsum = hasher.hexdigest()

        if filepath_is_hash:
            data_filepath = os.path.join(data_dir, hashsum)
            # replace placeholder
            n_placeholder = len("__HASHFILE__")
            norm_data_path = norm_data_path[:-n_placeholder] + hashsum

        if os.path.exists(data_filepath):
            os.remove(file.name)
            if filepath_is_hash:
                logging.debug(f"Skipping existing hashed file: {data_filepath}")
            else:
                raise IntegrityError(f"File should not exist: {data_filepath}")
        else:
            logging.debug(f"MOVE_TEMP {file.name} => {data_filepath}")
            os.rename(file.name, data_filepath)

        make_file_readonly(data_filepath)

        # write metadata
        metadata = {
            f"hash.{hash_method}": hashsum,
            "size": size,
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": norm_data_path,
        }
        self.metadata_put(data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def data_open(self, data_path: str, auto_load_uri: bool = False) -> BufferedReader:
        if auto_load_uri and not self.data_exists(data_path=data_path):
            uri = as_uri(source=data_path)
            data, metadata = open_uri(uri)
            norm_data_path = self.data_put(data=data, data_path=uri, exist_ok=False)
            if metadata:
                self.metadata_put(data_path=data_path, metadata=metadata)
        else:
            norm_data_path = self.data_exists(data_path=data_path)

        if not norm_data_path:
            raise DataDoesNotExists(data_path)

        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        logging.debug(f"READING {data_filepath}")
        data = open(data_filepath, "rb")

        return data

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

    def cache(
        self,
        get_path=None,
        from_bytes=None,
        to_bytes=None,
        path_prefix: str = None,
    ):
        """decorator to cache function results.

        Args:
            get_path(function): get path from (fun, args, kwargs)
            from_bytes(function): deserialize function results to bytes
            to_bytes(function): serialize function results to bytes
            path_prefix(str): path prefix for all cache results
        """
        return cache(
            storage=self,
            get_path=get_path,
            from_bytes=from_bytes,
            to_bytes=to_bytes,
            path_prefix=path_prefix,
        )
