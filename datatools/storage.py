import abc
import hashlib
import json
import logging
import os
import re
from io import BufferedReader
from tempfile import NamedTemporaryFile
from typing import Iterable, Union

import jsonpath_ng

from .constants import DEFAULT_HASH_METHOD, LOCAL_LOCATION, ROOT_METADATA_PATH
from .exceptions import DataDoesNotExists, DataExists, InvalidPath
from .utils import (
    as_byte_iterator,
    get_now_str,
    get_user_w_host,
    is_file_readonly,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)


class StorageAbstractBase(abc.ABC):
    @abc.abstractmethod
    def data_exists(self, data_path) -> bool:
        ...

    @abc.abstractmethod
    def data_delete(self, data_path: str, delete_metadata=False) -> None:
        ...

    @abc.abstractmethod
    def data_put(
        self,
        data: Union[BufferedReader, bytes, Iterable],
        data_path: str,
        exist_ok=False,
    ) -> str:
        ...

    @abc.abstractmethod
    def data_open(self, data_path: str) -> BufferedReader:
        ...

    @abc.abstractmethod
    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        ...

    @abc.abstractmethod
    def metadata_set(self, data_path: str, metadata: dict) -> None:
        ...


class StorageBase(StorageAbstractBase):
    def __init__(self, location=None):
        self.__location = os.path.abspath(location or LOCAL_LOCATION)

    @property
    def location(self):
        return self.__location

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

        # TODO: create function is_metadata_path
        if self._is_metadata_path(norm_data_path):
            raise InvalidPath(data_path)

        return norm_data_path

    def _is_metadata_path(self, path: str):
        return ".metadata." in path

    def _is_temp_path(self, path: str):
        return "+" in path

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern

    def _get_data_filepath(self, norm_data_path: str) -> str:
        data_filepath = os.path.join(self.location, norm_data_path)
        data_filepath = os.path.abspath(data_filepath)
        return data_filepath

    def _get_metadata_filepath(self, norm_data_path: str) -> str:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        metadata_filepath = os.path.abspath(metadata_filepath)
        return metadata_filepath

    def _get_existing_data_filepath(self, data_path: str) -> str:
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            return data_filepath
        raise DataDoesNotExists(data_path)

    def data_exists(self, data_path) -> bool:
        try:
            self._get_existing_data_filepath(data_path=data_path)
            return True
        except DataDoesNotExists:
            return False

    def data_delete(self, data_path: str, delete_metadata=False) -> None:
        try:
            data_filepath = self._get_existing_data_filepath(data_path=data_path)
        except DataDoesNotExists:
            return
        make_file_writable(file_path=data_filepath)
        logging.debug(f"DELETING {data_filepath}")
        os.remove(data_filepath)

        if delete_metadata:
            norm_data_path = self._get_norm_data_path(data_path=data_path)
            metadata_filepath = self._get_metadata_filepath(
                norm_data_path=norm_data_path
            )
            if os.path.exists(metadata_filepath):
                logging.debug(f"DELETING {metadata_filepath}")
                os.remove(metadata_filepath)

        return

    def data_put(
        self,
        data: Union[BufferedReader, bytes, Iterable],
        data_path: str,
        exist_ok=False,
    ) -> str:
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            if not exist_ok:
                raise DataExists(norm_data_path)
            logging.info(f"Skipping existing file: {norm_data_path}")
            return norm_data_path

        # write data
        tmp_dir = os.path.dirname(data_filepath)
        tmp_prefix = os.path.basename(data_filepath) + "+"

        hash_method = DEFAULT_HASH_METHOD
        hasher = getattr(hashlib, hash_method)()
        size = 0

        os.makedirs(tmp_dir, exist_ok=True)
        with NamedTemporaryFile(
            "wb", dir=tmp_dir, prefix=tmp_prefix, delete=False
        ) as file:
            logging.debug(f"WRITING {file.name}")
            for chunk in as_byte_iterator(data):
                file.write(chunk)
                size += len(chunk)
                hasher.update(chunk)

        try:
            logging.debug(f"MOVE {file.name} => {data_filepath}")
            os.rename(file.name, data_filepath)
        except Exception:
            logging.debug(f"DEL {file.name}")
            os.remove(file.name)
            raise
        make_file_readonly(data_filepath)

        metadata = {
            f"hash.{hash_method}": hasher.hexdigest(),
            "size": size,
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": norm_data_path,
        }

        self.metadata_set(data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def data_open(self, data_path: str) -> BufferedReader:
        data_filepath = self._get_existing_data_filepath(data_path=data_path)
        logging.debug(f"READING {data_filepath}")
        file = open(data_filepath, "rb")
        return file

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        metadata_path_pattern = self._create_metadata_path_pattern(
            metadata_path=metadata_path
        )
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

        if len(result) == 0:
            result = None
        elif len(result) == 1:
            result = result[0]
        else:
            logging.warning("multiple results in metadata found")

        logging.debug(f"get metadata: {metadata_path} => {result}")
        return result

    def metadata_set(self, data_path: str, metadata: dict) -> None:
        norm_data_path = self._get_norm_data_path(data_path=data_path)
        metadata_filepath = self._get_metadata_filepath(norm_data_path=norm_data_path)

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

        os.makedirs(os.path.dirname(metadata_filepath), exist_ok=True)
        logging.debug(f"WRITING {metadata_filepath}")
        with open(metadata_filepath, "wb") as file:
            file.write(metadata_bytes)


class Storage(StorageBase):
    def check(self, fix=False):
        """scan location and check for problems"""
        for rt, _ds, fs in os.walk(self.location):
            rt_rl = os.path.relpath(rt, self.location).replace("\\", "/")
            rt = rt.replace("\\", "/")
            for filename in fs:
                filepath = f"{rt}/{filename}"
                filepath_rel = f"{rt_rl}/{filename}"

                if self._is_metadata_path(filepath_rel):
                    continue

                if self._is_temp_path(filename):
                    logging.warning(f"Possibly incomplete tempfile: {filepath}")
                    continue

                logging.debug(filepath)

                if not is_file_readonly(filepath):
                    if fix:
                        make_file_readonly(filepath)
                        logging.info(f"FIXED: File not readonly: {filepath}")
                    else:
                        logging.warning(f"File not readonly: {filepath}")

                norm_path = self._get_norm_data_path(filepath_rel)

                # ignore case not now:
                if norm_path != filepath_rel:
                    data_path_old = self._get_data_filepath(filepath_rel)
                    data_path_new = self._get_data_filepath(norm_path)

                    meta_data_path_old = self._get_metadata_filepath(data_path_old)
                    meta_data_path_new = self._get_metadata_filepath(data_path_new)

                    if fix:
                        os.rename(data_path_old, data_path_new)
                        if os.path.exists(meta_data_path_old):
                            os.rename(meta_data_path_old, meta_data_path_new)
                        logging.info(f"FIXED: invalid path: {filepath_rel}")
                    else:
                        logging.warning(f"invalid path: {filepath_rel}")

    def search(self, *path_patterns) -> Iterable[str]:
        path_patterns = [re.compile(".*" + p.lower()) for p in path_patterns]
        for rt, _ds, fs in os.walk(self.location):
            rt_rl = os.path.relpath(rt, self.location).replace("\\", "/")
            for filename in fs:
                path = f"{rt_rl}/{filename}"
                if self._is_metadata_path(path):
                    continue
                if all(p.match(path) for p in path_patterns):
                    yield path
