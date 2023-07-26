import logging
import os
import re
from pathlib import Path
from typing import Tuple
from urllib.parse import parse_qs, urlencode, urlsplit

import pandas as pd
import requests
import sqlalchemy as sa

from .cache import DEFAULT_TO_BYTES
from .utils import (
    filepath_abs_to_uri,
    normalize_sql_query,
    parse_content_type,
    remove_auth_from_uri_or_path,
    uri_to_filepath_abs,
)

PARAM_SQL_QUERY = "q"


def get_table_schema(cursor):
    fields = [
        {"name": name, "data_type": data_type, "is_nullable": is_nullable}
        for (
            name,
            data_type,
            _display_size,
            _internal_size,
            _precision,
            _scale,
            is_nullable,
        ) in cursor.description
    ]
    return {"fields": fields}


def read_uri(uri: str) -> Tuple[bytes, str, dict]:
    metadata = {}

    metadata["source.path"] = remove_auth_from_uri_or_path(uri)

    url_parts = urlsplit(uri)

    # protocol routing
    if url_parts.scheme == "file":
        file_path = uri_to_filepath_abs(uri)
        with open(file_path, "rb") as file:
            data = file.read()
    elif url_parts.scheme in ["http", "https"]:
        res = requests.get(uri)
        res.raise_for_status()
        content_type = res.headers.get("Content-Type")
        if content_type:
            _meta = parse_content_type(content_type)
            metadata.update(_meta)
            logging.info(_meta)
        data = res.content
    elif "sql" in url_parts.scheme:
        # pop sql query
        query_dict = parse_qs(url_parts.query)
        sql_query = query_dict.pop(PARAM_SQL_QUERY)[0]
        sql_query = normalize_sql_query(sql_query)
        metadata["source.query"] = sql_query
        # doseq: if False: encode arrays differently
        query_str = urlencode(query_dict, doseq=True)
        url_parts = url_parts._replace(query=query_str)

        if not url_parts.netloc:
            # usually, netloc is empty, and so geturl() drops the "//"" at the beginning
            url_parts = url_parts._replace(path="//" + url_parts.path)
        # drop fragment
        url_parts = url_parts._replace(fragment=None)

        connection_string = url_parts.geturl()
        logging.debug(f"Connect: {connection_string}")
        eng = sa.create_engine(connection_string)
        with eng.connect() as con:
            with con:
                logging.debug(f"Exceute: {sql_query}")
                res = con.execute(sa.text(sql_query))
                data_schema = get_table_schema(res.cursor)
                logging.debug(f"Schema: {data_schema}")
                df = pd.DataFrame(res.fetchall())
                logging.debug(f"Rows: {len(df)}")
        # make sure everything is closed
        eng.dispose()

        data = DEFAULT_TO_BYTES(df)
        metadata["schema"] = data_schema

    else:
        raise NotImplementedError(url_parts.scheme)

    return data, metadata


def write_uri(uri, data: bytes):
    if not re.match(".+://", uri, re.IGNORECASE):
        # assume local path
        uri = filepath_abs_to_uri(Path(uri).absolute())

    url_parts = urlsplit(uri)
    # protocol routing
    if url_parts.scheme == "file":
        file_path = uri_to_filepath_abs(uri)
        if os.path.exist(file_path):
            raise FileExistsError(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(data)
    else:
        raise NotImplementedError(url_parts.scheme)