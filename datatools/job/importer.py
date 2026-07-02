"""Abstract classes / interfaces, types"""

from abc import ABC, abstractmethod
import re
from typing import Any
from urllib.parse import urlparse

import httpx
import pandas as pd
import sqlalchemy as sa
from typing_extensions import override

from datatools.types import UID
from datatools.utils import (
    get_uid_from_uri,
    is_file_uri_or_path,
    subclasses_by_name,
    uri_or_path_to_path,
)


class Importer(ABC):
    """TODO"""

    @classmethod
    def can_handle(cls, uri: str, **options) -> bool:
        """Can class handle uri"""
        return False

    @classmethod
    def get_output_uid(cls, uri: str, **options) -> str:
        """TODO"""
        return get_uid_from_uri(uri)

    @classmethod
    @abstractmethod
    def get_data(cls, uri: str, **options) -> Any: ...

    @classmethod
    def output_to_bytes(cls, data: Any) -> bytes:
        """convert result to bytes."""
        return data


def infer_importer_class(uri: str, **options) -> type[Importer]:
    """TODO"""

    for cls in subclasses_by_name(Importer).values():
        if cls.can_handle(uri, **options):
            return cls
    raise NotImplementedError(f"Cannot infer Importer class for {uri}")


class HttpImporter(Importer):
    """TODO"""

    @classmethod
    @override
    def can_handle(cls, uri: str, **options) -> bool:
        return bool(re.match(r"^https?://", uri))

    @classmethod
    def get_data(cls, uri: str, **options):
        """TODO"""
        resp = httpx.get(uri, follow_redirects=True)
        resp.raise_for_status()
        data = resp.content
        return data


class FileImporter(Importer):
    """TODO"""

    @classmethod
    def can_handle(cls, uri: str, **options) -> bool:
        """Either file:// protocol or no protocol"""
        return is_file_uri_or_path(uri)

    @classmethod
    def get_data(cls, uri: str, **options):
        """TODO"""
        path = uri_or_path_to_path(uri).resolve()
        data = path.read_bytes()
        return data

    @classmethod
    def get_output_uid(cls, uri: str, **options) -> UID:
        """TODO"""

        path = uri_or_path_to_path(uri).resolve()
        # FIXME:
        # we cant/dont want to use full path as UID
        # but we may want to use more than just the file name
        # but there is no good way to automatically determine what part
        # of the path to keep.
        # Maybe later, we can use a prefix option or something like that

        uid = path.name
        return uid


class SqlImporter(Importer):
    """TODO"""

    @classmethod
    @override
    def can_handle(cls, uri: str, **options) -> bool:
        parsed = urlparse(uri)
        return bool(parsed.scheme and "sql" in parsed.scheme.lower())

    @classmethod
    def get_data(cls, uri: str, query: str, **options):
        """TODO"""
        eng = sa.create_engine(uri)
        with eng.connect() as con:
            resp = con.execute(sa.text(query))
            data = resp.fetchall()

        return data

    @classmethod
    @override
    def output_to_bytes(cls, data: list):
        df = pd.DataFrame(data)
        data_s = df.to_csv(index=False)
        data_b = data_s.encode()
        return data_b
