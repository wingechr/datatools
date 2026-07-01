"""TODO"""

from collections.abc import Iterable
import json
import logging
import os
from pathlib import Path

import rdflib

from datatools.exceptions import StorageInvalidUidError
from datatools.storage.base import DataStorage
from datatools.storage.memory import PersistentMemoryMetadataStorage
from datatools.types import UID, MetadataValue
from datatools.utils import TextFile, is_file_uri_or_path, uri_or_path_to_path


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

    def __init__(self, path: Path, uid: UID):
        self.uid = uid
        self.context = {"@vocab": "urn:dummy/"}
        super().__init__(path)

    def _load_or_init(self) -> dict | None:
        data = super()._load_or_init()
        if not data:
            data = {"@id": self.uid, "@context": self.context}

        return data

    def _dump(self, data: dict) -> None:
        # rdf roundtrip test

        data_s = json.dumps(data)
        g = rdflib.Graph()
        g.parse(data=data_s, format="json-ld")
        data_s_new = g.serialize(format="json-ld", context=self.context)
        data_new = json.loads(data_s_new)

        super()._dump(data_new)

    def _as_uri(self, x: str) -> rdflib.URIRef:
        """FIXME"""
        return rdflib.URIRef("urn:" + x)

    def _as_uri_or_literal(self, x: MetadataValue) -> rdflib.URIRef | rdflib.Literal:
        """FIXME"""
        return rdflib.Literal(x)


class FileDataStorage(DataStorage):
    """TODO"""

    metadata_sufix = ".metadata.json"

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        """Either file:// protocol or no protocol"""
        return is_file_uri_or_path(location)

    def __init__(self, location: str = "."):
        path = uri_or_path_to_path(location).resolve()
        self._location: Path  # absolute, resolved location
        super().__init__(location=path)

    def _contains(self, uid: UID) -> bool:
        path = self._get_abs_path(uid)
        return path.exists()

    def _getitem(self, uid: UID) -> bytes:
        path = self._get_abs_path(uid)
        logging.debug("Reading %s", path)
        return path.read_bytes()

    def _setitem(self, uid: UID, data: bytes) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Writing %s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _delitem(self, uid: UID) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Deleting %s", path)
        os.remove(path)

    def _list(self) -> Iterable[UID]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                path = Path(root) / str(f)
                uid = str(path.relative_to(self._location))

                yield uid

    def _metadata(self, uid: UID) -> JsonFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonFileMetadataStorage(path_metadata)

    def _get_abs_path(self, uid: UID) -> Path:
        return (self._location / uid).resolve()

    def _get_valid_uid(self, uid: UID) -> UID:
        """should be a relative path"""
        uid = uid.strip()
        abs_path = self._get_abs_path(uid)
        if not abs_path.is_relative_to(self._location):
            raise StorageInvalidUidError(
                f"Cannot use uid outside of storage location: {uid}", uid=UID()
            )
        if abs_path.exists() and not abs_path.is_file():
            raise StorageInvalidUidError(f"uid is cannot be a file: {uid}", uid=UID())
        return abs_path.relative_to(self._location).as_posix()


class FileDataStorageWithRdfMetadata(FileDataStorage):
    """TODO"""

    def _metadata(self, uid: UID) -> JsonLdFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonLdFileMetadataStorage(path_metadata, uid=uid)
