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
from datatools.types import Name

sql_base = MetaData()

table_data = Table(
    "data",
    sql_base,
    Column("name", VARCHAR, primary_key=True),
    Column("data", VARBINARY),
)

table_metadata = Table(
    "metadata",
    sql_base,
    Column("name", VARCHAR, primary_key=True),
    Column("metadata", VARCHAR),  # or use JSON
)


class SqlMetadataStorage(PersistentMemoryMetadataStorage):
    """TODO"""

    def __init__(self, engine: Engine, name: Name):
        self._engine = engine
        self._name = name
        super().__init__()

    def _load_or_init(self) -> dict | None:
        with self._engine.begin() as con:
            rows = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.metadata)
                .where(
                    table_metadata.c.name == self._name,
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
                .with_only_columns(table_metadata.c.name)
                .where(table_metadata.c.name == self._name)
            )
            n_res = len(resp.fetchall())
            if n_res:
                # update
                con.execute(
                    table_metadata.update()
                    .values(metadata=data_s)
                    .where(table_metadata.c.name == self._name)
                )
            else:
                # insert
                con.execute(
                    table_metadata.insert().values(name=self._name, metadata=data_s)
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

    def _contains(self, name: Name) -> bool:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.name)
                .where(table_data.c.name == name)
            )
            n_res = len(resp.fetchall())
        return bool(n_res)

    def _getitem(self, name: Name) -> bytes:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.data)
                .where(table_data.c.name == name)
            )
            row = resp.fetchone()
            if not row:
                raise StorageFileNotFoundError(f"Not found: {name}")
            return row[0]

    def _setitem(self, name: Name, data: bytes) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.insert().values(name=name, data=data))

    def _delitem(self, name: Name) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.delete().where(table_data.c.name == name))

    def _list(self) -> Iterable[Name]:
        with self._engine.begin() as con:
            resp = con.execute(table_data.select().with_only_columns(table_data.c.name))
            names = {x[0] for x in resp.fetchall()}
        return names

    def _metadata(self, name: Name) -> MetadataStorage:
        return SqlMetadataStorage(engine=self._engine, name=name)
