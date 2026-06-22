"""TODO"""

from collections.abc import Iterable
import logging
import os
from pathlib import Path
from typing import Any, Literal

import httpx
from sqlalchemy import (
    VARBINARY,
    VARCHAR,
    Column,
    Engine,
    MetaData,
    Table,
    create_engine,
)

from ..utils import TextFile
from .types import (
    UID,
    DataStorage,
    MetadataAttribute,
    MetadataStorage,
    MetadataValue,
    StorageInvalidUidError,
)

metadata = MetaData()

table_data = Table(
    "data",
    metadata,
    Column("uid", VARCHAR, primary_key=True),
    Column("data", VARBINARY),
)
table_metadata = Table(
    "metadata",
    metadata,
    Column("uid", VARCHAR),
    Column("attribute", VARCHAR),
    Column("value", VARCHAR),
)


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, data: dict | None = None):
        self._data = {} if data is None else data

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        value = self._data.get(attribute)
        if value is None:
            return []
        else:
            return [value]

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._data[attribute] = value


class JsonFileMetadataStorage(MetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path):
        self._file = TextFile(path)
        self._storage = MemoryMetadataStorage(self._load_data())
        self._changed = False

    def _load_data(self) -> dict:
        if not self._file.exists():
            data = {}
        else:
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")
        return data

    def __del__(self):
        if self._changed:
            self._file.dump_json(self._storage._data)

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._storage._getitem(attribute)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._changed = True
        self._storage._setitem(attribute, value)


class MemoryDataStorage(DataStorage[Any]):
    """TODO"""

    def __init__(self):
        super().__init__(location=":memory")
        self.__data: dict[UID, Any] = {}
        self.__metadata: dict[UID, MemoryMetadataStorage] = {}

    def _contains(self, uid: UID) -> bool:
        return uid in self.__data

    def _getitem(self, uid: UID) -> Any:
        return self.__data[uid]

    def _setitem(self, uid: UID, data: Any) -> None:
        self.__data[uid] = data

    def _delitem(self, uid: UID) -> None:
        del self.__data[uid]
        # dont delete metadata

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        for uid in self.__data:
            if filters:
                metadata = self._metadata(uid)
                if not metadata._match(**filters):
                    continue

            yield uid

    def _metadata(self, uid: UID) -> MemoryMetadataStorage:
        if uid not in self.__metadata:
            self.__metadata[uid] = MemoryMetadataStorage()
        return self.__metadata[uid]


class FileDataStorage(DataStorage[bytes]):
    """TODO"""

    metadata_sufix = ".metadata.json"

    def __init__(self, location: str | None = None):
        self._location: Path  # absolute, resolved location
        super().__init__(location=Path(location or ".").resolve())

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

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                path = Path(root) / str(f)
                uid = str(path.relative_to(self._location))

                if filters:
                    metadata = self._metadata(uid)
                    if not metadata._match(**filters):
                        continue

                yield uid

    def _metadata(self, uid: UID) -> JsonFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonFileMetadataStorage(path_metadata)

    def _get_abs_path(self, uid: UID) -> Path:
        return (self._location / uid).resolve()

    def _get_valid_uid(self, uid: UID) -> UID:
        """should be a relative path"""
        abs_path = self._get_abs_path(uid)
        if not abs_path.is_relative_to(self._location):
            raise StorageInvalidUidError(
                f"Cannot use uid outside of storage location: {uid}", uid=UID()
            )
        if abs_path.exists() and not abs_path.is_file():
            raise StorageInvalidUidError(f"uid is cannot be a file: {uid}", uid=UID())
        return UID(abs_path.relative_to(self._location))


class HttpMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, url: str):
        self._location = url

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: dict | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, json=data)
        resp.raise_for_status()
        return resp

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._request(params={"a": attribute}).json()

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(method="POST", data={attribute: value})


class HttpDataStorage(DataStorage[bytes]):
    """TODO"""

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: bytes | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, content=data)
        resp.raise_for_status()
        return resp

    def _contains(self, uid: UID) -> bool:
        try:
            resp = self._request(path=f"/{uid}", method="HEAD")
            return resp.is_success
        except httpx.HTTPStatusError as exc:
            if not exc.response.status_code == 404:
                raise
        return False

    def _getitem(self, uid: UID) -> bytes:
        resp = self._request(path=f"/{uid}", method="GET")
        return resp.content

    def _setitem(self, uid: UID, data: bytes) -> None:
        self._request(path=f"/{uid}", method="PUT", data=data)

    def _delitem(self, uid: UID) -> None:
        self._request(path=f"/{uid}", method="DELETE")

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/", params={"q": filters_list}).json()

    def _metadata(self, uid: UID) -> HttpMetadataStorage:
        url = self._location + f"/{uid}/metadata"
        return HttpMetadataStorage(url)


class SqlMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, engine: Engine, uid: UID):
        self._engine = engine
        self._uid = uid

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        with self._engine.begin() as con:
            resp = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.value)
                .where(
                    table_metadata.c.uid == self._uid,
                    table_metadata.c.attribute == attribute,
                )
            )
            return [x[0] for x in resp.fetchall()]

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        with self._engine.begin() as con:
            con.execute(
                table_metadata.insert().values(
                    uid=self._uid, attribute=attribute, value=value
                )
            )


class SqlDataStorage(DataStorage[Any]):
    """TODO"""

    def __init__(self, location: str = "sqlite:///:memory:"):
        super().__init__(location=location)
        self._engine = create_engine(location)

        metadata.create_all(self._engine)

    def _contains(self, uid: UID) -> bool:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.uid)
                .where(table_data.c.uid == uid)
            )
            n_res = len(resp.fetchall())
        return bool(n_res)

    def _getitem(self, uid: UID) -> bytes:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.data)
                .where(table_data.c.uid == uid)
            )
            return resp.fetchone()[0]

    def _setitem(self, uid: UID, data: bytes) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.insert().values(uid=uid, data=data))

    def _delitem(self, uid: UID) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.delete().where(table_data.c.uid == uid))

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        with self._engine.begin() as con:
            resp = con.execute(table_data.select().with_only_columns(table_data.c.uid))
            uids = {x[0] for x in resp.fetchall()}
            # TODO query join directly in database
            for a, v in filters.items():
                print(con.execute(table_metadata.select()).fetchall())
                resp = con.execute(
                    table_metadata.select()
                    .with_only_columns(table_metadata.c.uid)
                    .where(table_metadata.c.attribute == a, table_metadata.c.value == v)
                )
                uids_filtered = {x[0] for x in resp.fetchall()}
                uids = uids & uids_filtered

        return uids

    def _metadata(self, uid: UID) -> MetadataStorage:
        return SqlMetadataStorage(engine=self._engine, uid=uid)
