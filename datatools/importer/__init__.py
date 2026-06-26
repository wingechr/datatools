"""init"""

from abc import ABC, abstractmethod
import re
from typing import Any, override
from urllib.parse import parse_qs, urlsplit, urlunsplit

import httpx
import pandas as pd
import sqlalchemy as sa

from datatools.types import UID
from datatools.utils import is_file_uri_or_path, subclasses_by_name, uri_or_path_to_path


class Importer(ABC):
    """TODO"""

    @classmethod
    def can_handle(cls, uri: str, **options) -> bool:
        """Can class handle uri"""
        return False

    @classmethod
    @abstractmethod
    def get_output_uid(cls, uri: str, **options) -> str: ...

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
    def can_handle(cls, uri: str) -> bool:
        return bool(re.match(r"^https?://", uri))

    @classmethod
    def get_data(cls, uri: str, **options):
        """TODO"""
        resp = httpx.get(uri)
        resp.raise_for_status()
        data = resp.content
        return data

    @classmethod
    def get_output_uid(cls, uri: str, **options) -> UID:
        """FIXME"""
        parts = urlsplit(uri)
        name = f"{parts.netloc}/{parts.path.strip('/')}"
        name = name.strip("/")
        return name


class FileImporter(Importer):
    """TODO"""

    @classmethod
    def can_handle(cls, uri: str) -> bool:
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
        """FIXME"""
        path = uri_or_path_to_path(uri).resolve()
        name = path.name
        return name


class SqlImporter(Importer):
    """TODO"""

    @classmethod
    @override
    def can_handle(cls, uri: str) -> bool:
        return bool(re.match(r"^.*sql.*://", uri))

    @classmethod
    def get_output_uid(cls, uri: str, **options) -> UID:
        """FIXME"""
        _cs, _query, uid = cls._get_cs_query_uid(uri, **options)
        return uid

    @classmethod
    def _get_cs_query_uid(cls, uri: str, **options) -> tuple[str, str, UID]:
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

    @classmethod
    def get_data(cls, uri: str, **options):
        """TODO"""
        cs, query, uid = cls._get_cs_query_uid(uri, **options)

        eng = sa.create_engine(cs)
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
