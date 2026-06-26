"""init"""

from abc import ABC, abstractmethod
from collections.abc import Callable
import re
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit, urlunsplit

import httpx
import pandas as pd
import sqlalchemy as sa

from datatools.types import UID, MetadataPairs, StorageFileExistsError
from datatools.utils import is_file_uri_or_path, subclasses_by_name, uri_or_path_to_path

if TYPE_CHECKING:
    from datatools.storage.classes import DataStorage


class Importer(ABC):
    """TODO"""

    @classmethod
    def _can_handle(cls, uri: str, **options) -> bool:
        return False

    def __init__(self, data_storage: "DataStorage", uri: str, **options):
        self._data_storage = data_storage
        self._options = options
        self._uri = uri

    @abstractmethod
    def _get_data_and_metadata(
        self, uri: str, **options
    ) -> tuple[bytes, MetadataPairs]: ...

    @abstractmethod
    def _get_output_uid(self, uri: str, **options) -> UID: ...

    def _get_valid_output_uid(self, uri: str, **options) -> UID:
        output_uid = self._get_output_uid(self._uri, **self._options)
        if output_uid in self._data_storage:
            raise StorageFileExistsError(output_uid)
        return output_uid

    def __call__(self) -> UID:
        """TODO

        maybe we can use storage.job()
        """
        output_uid = self._get_valid_output_uid(self._uri, **self._options)
        data, metadata = self._get_data_and_metadata(self._uri, **self._options)
        self._data_storage[output_uid] = data
        metadata_storage = self._data_storage.metadata(output_uid)
        for k, v in metadata:
            metadata_storage[k] = v
        return output_uid


class FileImporter(Importer):
    """TODO"""

    @classmethod
    def _can_handle(cls, uri: str) -> bool:
        """Either file:// protocol or no protocol"""
        return is_file_uri_or_path(uri)

    def _get_data_and_metadata(
        self, uri: str, **options
    ) -> tuple[bytes, MetadataPairs]:
        path = uri_or_path_to_path(uri).resolve()
        print((uri, path))
        data = path.read_bytes()
        metadata = [("source", uri)]
        return data, metadata

    def _get_output_uid(self, uri: str, **options) -> UID:
        """FIXME"""
        path = uri_or_path_to_path(uri).resolve()
        name = path.name
        return name


class HttpImporter(Importer):
    """TODO"""

    @classmethod
    def _can_handle(cls, uri: str) -> bool:
        return bool(re.match(r"^https?://", uri))

    def _get_data_and_metadata(
        self, uri: str, **options
    ) -> tuple[bytes, MetadataPairs]:
        resp = httpx.get(uri)
        resp.raise_for_status()
        data = resp.content
        metadata = [("source", uri), ("content_type", resp.headers.get("Content-type"))]
        return data, metadata

    def _get_output_uid(self, uri: str, **options) -> UID:
        """FIXME"""
        parts = urlsplit(uri)
        name = f"{parts.netloc}/{parts.path.strip('/')}"
        name = name.strip("/")
        return name


class SqlImporter(Importer):
    """TODO"""

    def __init__(self, data_storage: "DataStorage", uri: str, **options):
        super().__init__(data_storage, uri=uri, **options)

    @classmethod
    def _can_handle(cls, uri: str) -> bool:
        return bool(re.match(r"^.*sql.*://", uri))

    def _get_cs_query_uid(self, uri: str, **options) -> tuple[str, str, UID]:
        parts = urlsplit(uri)
        _fragment = parts.fragment
        _query = parse_qs(parts.query)
        parts = parts._replace(fragment=None, query=None)

        query = _query["q"][0]

        cs = urlunsplit(parts)
        # special case sqlite:
        if cs == "sqlite:/:memory:":
            cs = "sqlite:///:memory:"

        uid = "data.csv"  # FIXME

        return cs, query, uid

    def _get_data_and_metadata(
        self, uri: str, **options
    ) -> tuple[bytes, MetadataPairs]:
        cs, query, uid = self._get_cs_query_uid(uri, **options)
        to_bytes = self._get_to_bytes(uid)

        eng = sa.create_engine(cs)
        with eng.connect() as con:
            resp = con.execute(sa.text(query))
            data = to_bytes(resp)

        metadata = [
            ("connection_string", cs),
            ("query", query),
        ]
        return data, metadata

    def _get_output_uid(self, uri: str, **options) -> UID:
        """FIXME"""
        _cs, _query, uid = self._get_cs_query_uid(uri, **options)
        return uid

    def _get_to_bytes(self, uid: UID) -> Callable[[sa.CursorResult], bytes]:
        def dump(res: sa.CursorResult) -> bytes:
            df = pd.DataFrame(res)
            data_s = df.to_csv(index=False)
            data_b = data_s.encode()
            return data_b

        return dump


def infer_importer_class(uri: str, **options) -> type[Importer]:
    """TODO"""
    for cls in subclasses_by_name(Importer).values():
        if cls._can_handle(uri, **options):
            return cls
    raise NotImplementedError(f"Cannot infer Importer class for {uri}")
