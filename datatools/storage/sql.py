"""TODO"""

from collections.abc import Iterable
import json
import re

from sqlalchemy import (
    VARBINARY,
    VARCHAR,
    Column,
    Engine,
    MetaData,
    Table,
    create_engine,
)

from datatools.exceptions import StorageFileNotFoundError
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.storage.memory import PersistentMemoryMetadataStorage
from datatools.types import UID

sql_base = MetaData()

table_data = Table(
    "data",
    sql_base,
    Column("uid", VARCHAR, primary_key=True),
    Column("data", VARBINARY),
)

table_metadata = Table(
    "metadata",
    sql_base,
    Column("uid", VARCHAR, primary_key=True),
    Column("metadata", VARCHAR),  # or use JSON
)


class SqlMetadataStorage(PersistentMemoryMetadataStorage):
    """TODO"""

    def __init__(self, engine: Engine, uid: UID):
        self._engine = engine
        self._uid = uid
        super().__init__()

    def _load_or_init(self) -> dict | None:
        with self._engine.begin() as con:
            rows = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.metadata)
                .where(
                    table_metadata.c.uid == self._uid,
                )
            ).fetchall()
        if rows:
            data_s = rows[0][0]
            return json.loads(data_s)

    def _dump(self, data: dict) -> None:
        data_s = json.dumps(data, ensure_ascii=False)
        # check if row exists
        with self._engine.begin() as con:
            resp = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.uid)
                .where(table_metadata.c.uid == self._uid)
            )
            n_res = len(resp.fetchall())
            if n_res:
                # update
                con.execute(
                    table_metadata.update()
                    .values(metadata=data_s)
                    .where(table_metadata.c.uid == self._uid)
                )
            else:
                # insert
                con.execute(
                    table_metadata.insert().values(uid=self._uid, metadata=data_s)
                )


class SqlDataStorage(DataStorage):
    """TODO

    TODO: for faster query, we should split structured data
    into triples? for now: we only use Json

    """

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return bool(re.match(r"^.*sql.*://", location))

    def __init__(self, location: str = "sqlite:///:memory:"):
        super().__init__(location=location)
        self._engine = create_engine(location)

        sql_base.create_all(self._engine)

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
            row = resp.fetchone()
            if not row:
                raise StorageFileNotFoundError(f"Not found: {uid}")
            return row[0]

    def _setitem(self, uid: UID, data: bytes) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.insert().values(uid=uid, data=data))

    def _delitem(self, uid: UID) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.delete().where(table_data.c.uid == uid))

    def _list(self) -> Iterable[UID]:
        with self._engine.begin() as con:
            resp = con.execute(table_data.select().with_only_columns(table_data.c.uid))
            uids = {x[0] for x in resp.fetchall()}
        return uids

    def _metadata(self, uid: UID) -> MetadataStorage:
        return SqlMetadataStorage(engine=self._engine, uid=uid)
