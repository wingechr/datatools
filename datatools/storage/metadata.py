import logging
import os
import json
import re
import sqlite3

from datatools.utils import normalize_name, get_unix_utc

from .exceptions import ObjectNotFoundException


class AbstractMetadataStorage:
    def set(self, file_id, identifier_values, user, unix_utc):
        """
        Args:
            file_id(str): 32 character md5 hash
            identifier_values(dict): identifier must be max 128 characters, values must be json serializable
            user(str): user identification / source
            unix_utc(float): unix timestamp (UTC)

        Returns:
            dataset_id(int)

        """
        raise NotImplementedError()

    def get(self, file_id, identifier):
        """
        Args:
            file_id(str): 32 character md5 hash
            identifier(str): max 128 character valid identifier

        Returns:
            value(object)

        Raises:
            ObjectNotFoundException
        """
        raise NotImplementedError()


class SqliteMetadataStorage(AbstractMetadataStorage):
    DEFAULT_DATABASE = ".metadata.sqlite3"
    DEFAULT_USER = None

    def __init__(self, database=None, default_user=None):
        self.database = os.path.abspath(database or self.DEFAULT_DATABASE)
        self.default_user = default_user or self.DEFAULT_USER

        os.makedirs(os.path.dirname(self.database), exist_ok=True)
        if os.path.isfile(self.database):
            logging.debug("using database: %s", self.database)
            init_sql = None
        else:
            logging.debug("creating database: %s", self.database)
            init_sql = [
                """
            create table dataset(
                dataset_id INTEGER PRIMARY KEY,
                file_id CHAR(32) NOT NULL,
                user VARCHAR(128) NOT NULL,
                unix_utc DECIMAL(16, 6) NOT NULL
            );""",
                """
            create table metadata(
                dataset_id INTEGER NOT NULL,
                identifier varchar(128) NOT NULL,
                value_json text NOT NULL,
                PRIMARY KEY(dataset_id, identifier),
                FOREIGN KEY(dataset_id) REFERENCES dataset(dataset_id)
            );
            """,
            ]
        self.connection = None

        if init_sql:
            with self:
                for sql in init_sql:
                    self._execute(sql)

    def __enter__(self):
        self.connection = sqlite3.connect(self.database)

    def __exit__(self, *args):
        self.connection.close()
        self.connection = None

    def _execute(self, sql, parameters=None):
        sql = re.sub("\s+", " ", sql).strip()
        if parameters:
            logging.debug("EXECUTE: %s %s", sql, parameters)
            return self.connection.cursor().execute(sql, parameters)
        else:
            logging.debug("EXECUTE: %s", sql)
            return self.connection.cursor().execute(sql)

    def _create_dataset(self, file_id, user=None, unix_utc=None):
        """Returns dataset_id"""
        unix_utc = unix_utc or get_unix_utc()
        user = user or self.default_user
        stmt = """SELECT MAX(dataset_id) FROM dataset;"""
        max_dataset_id = self._execute(stmt).fetchone()[0] or 0
        dataset_id = max_dataset_id + 1
        stmt = """INSERT INTO dataset(dataset_id, file_id, user, unix_utc) VALUES(?, ?, ?, ?);"""
        self._execute(stmt, [dataset_id, file_id, user, unix_utc])
        return dataset_id

    def set(self, file_id, identifier_values, user=None, unix_utc=None):
        dataset_id = self._create_dataset(file_id, user=user, unix_utc=unix_utc)
        stmt = """INSERT INTO metadata(dataset_id, identifier, value_json) VALUES(?, ?, ?);"""
        for identifier, value in identifier_values.items():
            identifier = normalize_name(identifier)
            value_json = json.dumps(value, sort_keys=True, ensure_ascii=False)
            self._execute(stmt, [dataset_id, identifier, value_json])

    def get(self, file_id, identifier):
        identifier = normalize_name(identifier)
        stmt = """
        SELECT value_json 
        FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
        WHERE d.file_id = ? AND identifier = ? AND unix_utc = (
            SELECT MAX(d.unix_utc) 
            FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
            WHERE d.file_id = ? AND identifier = ?
        )        
        """
        cur = self._execute(stmt, [file_id, identifier, file_id, identifier]).fetchone()
        if not cur:
            raise ObjectNotFoundException((file_id, identifier))
        value_json = cur[0]
        value = json.loads(value_json)
        return value
