"""TODO"""

from collections.abc import Iterable
import logging
import os
from pathlib import Path

import rdflib

from datatools.exceptions import StorageInvalidNameError
from datatools.storage.base import DataStorage
from datatools.storage.memory import PersistentMemoryMetadataStorage
from datatools.types import Name
from datatools.utils import TextFile, json_dumps, json_loads, uri_or_path_to_path


class JsonFileMetadataStorage(PersistentMemoryMetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path):
        self._file = TextFile(path)
        super().__init__()

    def _load_or_init(self) -> dict | None:
        if self._file.exists():
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")
            return data

    def _dump(self, data: dict) -> None:
        self._file.dump_json(data)


class JsonLdFileMetadataStorage(JsonFileMetadataStorage):
    """FIXME

    this is all still very experimental: clients expect to use
    jsonpath queries, so we convert from / to json

    for now, we just parse json as jsonld and back to see if its possible

    """

    def __init__(self, path: Path, name: Name):
        self.name = name
        self.context = {"@vocab": "urn:dummy/"}
        super().__init__(path)

    def _load_or_init(self) -> dict | None:
        data = super()._load_or_init()
        if not data:
            data = {"@id": self.name, "@context": self.context}

        return data

    def _dump(self, data: dict) -> None:
        # rdf roundtrip test

        data_s = json_dumps(data)
        g = rdflib.Graph()
        g.parse(data=data_s, format="json-ld")
        data_s_new = g.serialize(format="json-ld", context=self.context)
        data_new: dict = json_loads(data_s_new)  # type:ignore

        super()._dump(data_new)


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

    def _contains(self, name: Name) -> bool:
        path = self._get_abs_path(name)
        return path.exists()

    def _getitem(self, name: Name) -> bytes:
        path = self._get_abs_path(name)
        logging.debug("Reading %s", path)
        return path.read_bytes()

    def _setitem(self, name: Name, data: bytes) -> None:
        path = self._get_abs_path(name)
        logging.debug("Writing %s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _delitem(self, name: Name) -> None:
        path = self._get_abs_path(name)
        logging.debug("Deleting %s", path)
        os.remove(path)

    def _list(self) -> Iterable[Name]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
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
        return abs_path.relative_to(self._location).as_posix()


class FileDataStorageWithRdfMetadata(FileDataStorage):
    """TODO"""

    def _metadata(self, name: Name) -> JsonLdFileMetadataStorage:
        path = self._get_abs_path(name)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonLdFileMetadataStorage(path_metadata, name=name)
