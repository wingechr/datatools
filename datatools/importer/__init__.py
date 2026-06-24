"""init"""

from collections.abc import Callable
import re
from urllib.parse import parse_qs, urlsplit, urlunsplit

import httpx
import pandas as pd
import sqlalchemy as sa

from datatools.types import UID, DataStorage, Importer, MetadataPairs


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
        name = f"{parts.netloc}/{parts.path}"
        name = name.strip("/")
        return name


class SqlImporter(Importer):
    """TODO"""

    def __init__(self, data_storage: DataStorage, uri: str, **options):
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
