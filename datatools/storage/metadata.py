import logging
import os
import re
import datetime
import sqlite3

from datatools.utils import (
    normalize_name,
    get_timestamp_utc,
    json_dumps,
    get_data_hash,
    json_loads,
    strptime,
    get_user_host,
)
from .exceptions import ObjectNotFoundException, validate_file_id, InvalidValueException


class AbstractMetadataStorage:

    default_user = None

    def set_metadata(self, file_id, identifier_values, user=None, timestamp_utc=None):
        """
        Args:
            file_id(str): 32 character md5 hash
            identifier_values(dict): identifier must be max 128 characters, values must be json serializable
            user(str): user identification / source
            timestamp_utc(float): unix timestamp (UTC)

        Returns:
            dataset_id(int)

        """
        file_id = validate_file_id(file_id)
        timestamp_utc = validate_timestamp_utc(timestamp_utc or get_timestamp_utc())
        user = validate_user(user or self.default_user)
        identifier_values = validate_identifier_values(identifier_values)
        return self._set(file_id, identifier_values, user, timestamp_utc)

    def get_metadata(self, file_id, identifier):
        """
        Args:
            file_id(str): 32 character md5 hash
            identifier(str): max 128 character valid identifier

        Returns:
            value(object)

        Raises:
            ObjectNotFoundException
        """
        file_id = validate_file_id(file_id)
        identifier = validate_identifier(identifier)
        value_json = self._get(file_id, identifier)
        value = json_loads(value_json)
        return value

    def _set(self, file_id, identifier_values, user, timestamp_utc):
        raise NotImplementedError()

    def _get(self, file_id, identifier):
        raise NotImplementedError()

    def __enter__(self):
        raise NotImplementedError()

    def __exit__(self, *args):
        raise NotImplementedError()

    def _get_dataset_id(self, file_id, user, timestamp_utc, identifier_values):
        """Returns dataset_id"""
        dataset = {
            "file_id": file_id,
            "user": user,
            "timestamp_utc": timestamp_utc,
            "data": identifier_values,
        }
        dataset_id = get_data_hash(dataset)
        return dataset_id


def validate_timestamp_utc(timestamp_utc):
    if isinstance(timestamp_utc, str):
        timestamp_utc = strptime(timestamp_utc)
    elif not isinstance(timestamp_utc, datetime.datetime):
        raise InvalidValueException(timestamp_utc)
    return timestamp_utc


def validate_non_empty_str(x, max_len=None):
    if not isinstance(x, str):
        raise InvalidValueException(x)
    x = x.strip()
    if not x:
        raise InvalidValueException(x)
    if max_len and len(x) > max_len:
        raise InvalidValueException(x)
    return x


def validate_user(user):
    return validate_non_empty_str(user, 128)


def validate_identifier(identifier):
    identifier = normalize_name(identifier)
    return validate_non_empty_str(identifier, 128)


def validate_identifier_values(identifier_values):
    result = dict()
    for identifier, value in identifier_values.items():
        identifier = validate_identifier(identifier)
        if identifier in result:
            raise InvalidValueException(identifier)
        value = json_dumps(value)
        result[identifier] = value
    return result


class SqliteMetadataStorage(AbstractMetadataStorage):
    DEFAULT_DATABASE = ".metadata.sqlite3"
    DEFAULT_USER = get_user_host()

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
                dataset_id CHAR(32) PRIMARY KEY,
                file_id CHAR(32) NOT NULL,
                user VARCHAR(128) NOT NULL,
                timestamp_utc DATETIME NOT NULL,
                UNIQUE(file_id, timestamp_utc)
            );""",
                """
            create table metadata(
                dataset_id CHAR(32) NOT NULL,
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
        self.connection.commit()
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

    def _set(self, file_id, identifier_values, user=None, timestamp_utc=None):

        dataset_id = self._get_dataset_id(
            file_id, user, timestamp_utc, identifier_values
        )

        stmt = """INSERT INTO dataset(dataset_id, file_id, user, timestamp_utc) VALUES(?, ?, ?, ?);"""
        try:
            self._execute(stmt, [dataset_id, file_id, user, timestamp_utc])
        except sqlite3.IntegrityError:
            # already in database
            return dataset_id

        stmt = """INSERT INTO metadata(dataset_id, identifier, value_json) VALUES(?, ?, ?);"""
        for identifier, value_json in identifier_values.items():
            self._execute(stmt, [dataset_id, identifier, value_json])

        return dataset_id

    def _get(self, file_id, identifier):
        stmt = """
        SELECT value_json 
        FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
        WHERE d.file_id = ? AND identifier = ? AND timestamp_utc = (
            SELECT MAX(d.timestamp_utc) 
            FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
            WHERE d.file_id = ? AND identifier = ?
        )        
        """
        cur = self._execute(stmt, [file_id, identifier, file_id, identifier]).fetchone()
        if not cur:
            raise ObjectNotFoundException((file_id, identifier))
        value_json = cur[0]
        return value_json

    def get_all_metadata(self, file_id):
        file_id = validate_file_id(file_id)

        stmt = """
        SELECT m.identifier, m.value_json
        FROM metadata m 
        JOIN dataset d ON m.dataset_id = d.dataset_id
        JOIN (            
            SELECT m.identifier, MAX(d.timestamp_utc) as timestamp_utc
            FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
            WHERE d.file_id = ?
            group by m.identifier
        ) t on t.identifier = m.identifier and t.timestamp_utc = d.timestamp_utc        
        """
        result = {}
        for identifier, value_json in self._execute(stmt, [file_id]).fetchall():
            result[identifier] = json_loads(value_json)
        return result

    def get_all_extended(self, file_id):
        file_id = validate_file_id(file_id)

        stmt = """
        SELECT m.identifier, m.value_json, d.timestamp_utc, d.user
        FROM metadata m 
        JOIN dataset d ON m.dataset_id = d.dataset_id
        JOIN (            
            SELECT m.identifier, MAX(d.timestamp_utc) as timestamp_utc
            FROM metadata m JOIN dataset d ON m.dataset_id = d.dataset_id
            WHERE d.file_id = ?
            group by m.identifier
        ) t on t.identifier = m.identifier and t.timestamp_utc = d.timestamp_utc        
        """
        result = []
        for identifier, value_json, timestamp_utc, user in self._execute(
            stmt, [file_id]
        ).fetchall():
            value = json_loads(value_json)
            result.append(
                {
                    "identifier": identifier,
                    "value": value,
                    "timestamp_utc": timestamp_utc,
                    "user": user,
                }
            )
        return result
