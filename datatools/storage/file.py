"""TODO"""

from collections.abc import Iterable
import logging
import os
from pathlib import Path

import rdflib

from datatools.exceptions import StorageInvalidNameError
from datatools.io import JsonIO
from datatools.storage.base import DataStorage
from datatools.storage.memory import PersistentMemoryMetadataStorage
from datatools.types import (
    DEFAULT_CHUNK_SIZE,
    LOCKFILE_SUFFIX,
    RDF_CONTEXT,
    TEMPFILE_SUFFIX,
    Name,
)
from datatools.utils import (
    buffer_to_byte_iterable,
    make_file_readonly,
    make_file_writable,
    uri_or_path_to_path,
    write_bytes_locked,
)


class JsonFileMetadataStorage(PersistentMemoryMetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path):
        self._path = path
        super().__init__()

    def _load_or_init(self) -> dict | None:
        if self._path.exists():
            with self._path.open("rb") as file:
                data = JsonIO.load(file)
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")
            return data

    def _dump(self, data: dict) -> None:
        # data = self._test_roundtrip_rdf(data)
        bdata = JsonIO.dumpb(data)
        write_bytes_locked(self._path, [bdata])

    @staticmethod
    def _run_through_rdf(data: dict) -> dict:
        """TODO"""
        data_s = JsonIO.dumps(data)
        g = rdflib.Graph()
        g.parse(data=data_s, format="json-ld")
        data_s_new = g.serialize(
            format="json-ld", context=RDF_CONTEXT, auto_compact=True
        )
        data_new: dict = JsonIO.loads(data_s_new)
        return data_new


class FileDataStorage(DataStorage):
    """TODO"""

    metadata_sufix = ".metadata.json"

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        """Either file:// protocol or no protocol"""
        return Path(location).is_dir()

    def __init__(self, location: str = "."):
        path = uri_or_path_to_path(location).resolve()
        self._location: Path  # absolute, resolved location
        super().__init__(location=path)

    def _has(self, name: Name) -> bool:
        path = self._get_abs_path(name)
        return path.exists()

    def _read(
        self, name: Name, chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> Iterable[bytes]:
        path = self._get_abs_path(name)
        logging.debug("Reading %s", path)
        with path.open("rb") as file:
            yield from buffer_to_byte_iterable(file, chunk_size=chunk_size)

    def _write(self, name: Name, bytes_iter: Iterable[bytes]) -> None:
        path = self._get_abs_path(name)
        logging.debug("Writing %s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_locked(
            path=path,
            bytes_iter=bytes_iter,
            tempfile_suffix=TEMPFILE_SUFFIX,
            lockfile_suffix=LOCKFILE_SUFFIX,
        )
        make_file_readonly(path)

    def _delete(self, name: Name) -> None:
        path = self._get_abs_path(name)
        logging.debug("Deleting %s", path)
        make_file_writable(path)
        os.remove(path)

    def _list(self) -> Iterable[Name]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                if f.endswith(LOCKFILE_SUFFIX) or f.endswith(TEMPFILE_SUFFIX):
                    continue
                path = Path(root) / str(f)
                name = str(path.relative_to(self._location))

                yield name

    def _metadata(self, name: Name) -> JsonFileMetadataStorage:
        path = self._get_abs_path(name)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonFileMetadataStorage(path_metadata)

    def _get_abs_path(self, name: Name) -> Path:
        return (self._location / name).resolve()

    def _get_valid_name(self, name: Name) -> Name:
        """should be a relative path"""
        name = name.strip()
        abs_path = self._get_abs_path(name)

        if not abs_path.is_relative_to(self._location):
            raise StorageInvalidNameError(
                f"Cannot use name outside of storage location: {name}", name=Name()
            )
        if abs_path.exists() and not abs_path.is_file():
            raise StorageInvalidNameError(f"name must be a file: {name}", name=Name())

        if (
            name.endswith(self.metadata_sufix)
            or name.endswith(LOCKFILE_SUFFIX)
            or name.endswith(TEMPFILE_SUFFIX)
        ):
            raise StorageInvalidNameError(f"Reserved name: {name}", name=Name())

        return abs_path.relative_to(self._location).as_posix()
