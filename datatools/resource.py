import bz2
import gzip
import json
import logging
import os
import re
import zipfile
from io import BufferedReader, BytesIO
from typing import Callable, Tuple
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import geopandas as gpd
import pandas as pd
import pyproj
import requests
import sqlalchemy as sa
import xarray as xr
from bs4 import BeautifulSoup

from . import storage
from .cache import DEFAULT_MEDIA_TYPE, DEFAULT_TO_BYTES
from .utils import (
    as_uri,
    get_sql_table_schema,
    load_asciigrid,
    normalize_path,
    normalize_sql_query,
    parse_content_type,
    remove_auth_from_uri_or_path,
    uri_to_filepath_abs,
)

PARAM_SQL_QUERY = "q"


def get_default_storage():
    return storage.Storage(location=None)


class Metadata:
    def __init__(self, resource: "Resource"):
        self.__resource = resource

    def __getitem__(self, metadata_path):
        return self.__resource.storage.metadata_get(
            data_path=self.__resource.name, metadata_path=metadata_path
        )

    def __setitem__(self, metadata_path, value):
        metadata = {metadata_path: value}
        return self.__resource.storage.metadata_set(
            data_path=self.__resource.name, metadata=metadata
        )

    def get(self, metadata_path, default_value=None, save=False):
        result = self[metadata_path]
        if result is None:
            if isinstance(default_value, Callable):
                # pass resource
                result = default_value(self.__resource)
                if result is not None:
                    logging.debug(f"meta data from function: {metadata_path}={result}")
            else:
                result = default_value
                if result is not None:
                    logging.debug(f"meta data from default: {metadata_path}={result}")
            if result is not None and save:
                self[metadata_path] = result
        return result


class Resource:
    def __init__(self, uri: str, name: str = None, storage: "storage.Storage" = None):
        self.__storage = storage or get_default_storage()

        # TODO
        uri = uri or "data:///" + name

        self.__uri = as_uri(uri)
        self.__name = normalize_path(name or uri)

        url_parts = urlsplit(self.uri)

        self.__scheme = url_parts.scheme
        self.__netloc = url_parts.netloc
        self.__path = url_parts.path
        self.__query = parse_qs(url_parts.query)
        self.__fragment = url_parts.fragment

    def __str__(self):
        return (
            f"Resource(uri='{self.uri}', name='{self.name}', storage='{self.storage}')"
        )

    @property
    def metadata(self):
        return Metadata(resource=self)

    @property
    def scheme(self):
        return self.__scheme

    @property
    def netloc(self):
        return self.__netloc

    @property
    def path(self):
        return self.__path

    @property
    def query(self):
        return self.__query

    @property
    def fragment(self):
        return self.__fragment

    @property
    def uri(self):
        return self.__uri

    @property
    def name(self):
        return self.__name

    @property
    def storage(self):
        return self.__storage

    @property
    def filepath(self):
        return self.storage._get_existing_data_filepath(data_path=self.name)

    def _open(self, **metadata_kwargs) -> Tuple[BufferedReader, dict]:
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(self.uri)

        # protocol routing
        if self.scheme == "file":
            file_path = uri_to_filepath_abs(self.uri)
            logging.debug(f"OPEN: {file_path}")
            data = open(file_path, "rb")

        elif self.scheme in ["http", "https"]:
            # because we want to encode auth headers
            # in the uri, we place it in the netloc
            # part before the @host (instead of the basic auth)
            # if it's basic auth, the pattern is `user:pass`
            # if it's a header, it's header=value
            match_header = re.match("^([^=@]+)=([^=@]+)@(.+)$", self.netloc)
            headers = {}
            if match_header:
                h_name, h_val, netloc = match_header.groups()
                h_name = unquote(h_name)
                h_val = unquote(h_val)
                headers[h_name] = h_val
                logging.debug("Stripping auth header from uri")

            else:
                netloc = self.netloc

            # TODO: is self.query encoded properly automatically?

            query = urlencode(self.query, doseq=True) if self.query else None
            url = urlunsplit([self.scheme, netloc, self.path, query, None])
            logging.debug(f"OPEN: {url}")
            res = requests.get(url, stream=True, headers=headers)

            res.raise_for_status()
            content_type = res.headers.get("Content-Type")
            if content_type:
                _meta = parse_content_type(content_type)
                metadata.update(_meta)
                logging.info(_meta)

            data = res.raw

        elif "sql" in self.scheme:
            # pop sql query
            query_dict = self.query
            sql_query = query_dict.pop(PARAM_SQL_QUERY)[0]
            sql_query = unquote(sql_query)
            sql_query = normalize_sql_query(sql_query)

            metadata["source.query"] = sql_query

            # usually, netloc is empty, and so urlunsplit()
            # drops the "//"" at the beginning
            path = self.path if self.netloc else "//" + self.path
            # doseq: if False: encode arrays differently
            query_str = urlencode(query_dict, doseq=True)

            connection_string = urlunsplit(
                [self.scheme, self.netloc, path, query_str, None]
            )
            logging.debug(f"Connect: {connection_string}")
            eng = sa.create_engine(connection_string)
            with eng.connect() as con:
                with con:
                    logging.debug(f"Exceute: {sql_query}")
                    res = con.execute(sa.text(sql_query))
                    data_schema = get_sql_table_schema(res.cursor)
                    logging.debug(f"Schema: {data_schema}")
                    data = [rec._asdict() for rec in res.fetchall()]
                    logging.debug(f"Rows: {len(data)}")
            # make sure everything is closed
            eng.dispose()

            data = DEFAULT_TO_BYTES(data)
            data = BytesIO(data)

            metadata["schema"] = data_schema
            metadata["mediatype"] = DEFAULT_MEDIA_TYPE

        else:
            raise NotImplementedError(self.scheme)

        return data, metadata

    def _write(self, data: BufferedReader):
        # protocol routing
        if self.scheme == "file":
            file_path = uri_to_filepath_abs(self.uri)
            if os.path.exist(file_path):
                raise FileExistsError(file_path)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            logging.debug(f"WRTIE: {file_path}")
            with open(file_path, "wb") as file:
                file.write(data.read())
        else:
            raise NotImplementedError(self.scheme)

    def _save_if_not_exist(self):
        if not self.storage.data_exists(self.name):
            metadata_old = self.storage.metadata_get(data_path=self.name) or {}
            # use old metadata as opening hints
            data, metadata = self._open(**metadata_old)
            norm_name = self.storage.data_put(data=data, data_path=self.name)
            if norm_name != self.name:
                logging.warning(
                    f"storage uses different name {norm_name} instead of {self.name}"
                )
            self.storage.metadata_set(data_path=self.name, metadata=metadata)

    def open(self) -> BufferedReader:
        self._save_if_not_exist()
        return self.storage.data_open(data_path=self.name)

    @property
    def is_in_storage(self) -> bool:
        return self.storage.data_exists(data_path=self.name)

    def remove_from_storage(self, delete_metadata=False) -> None:
        self.storage.data_delete(data_path=self.name, delete_metadata=delete_metadata)

    # geopandas

    def _load_from_zip(self, load):
        name = self.name
        assumed_filename_in_zip = name.split("/")[-1]
        with self.open() as file:
            zfile = zipfile.ZipFile(file)
            # find
            filename_in_zip = None
            for zf in zfile.filelist:
                logging.debug(zf.filename)
                if normalize_path(zf.filename) + ".zip" == assumed_filename_in_zip:
                    filename_in_zip = zf.filename
                    break
            if not filename_in_zip:
                raise Exception(f"cannot find file in zip: {name}")
            file_in_zip = zfile.open(filename_in_zip)
            return load(file_in_zip)

    def load(self):
        # determine load method by media type
        name = self.name

        logging.info(name)
        if re.match(r".*\.(zip)$", name):
            if re.match(r".*\.(shp).zip$", name):
                return gpd.read_file(self.filepath)
            elif re.match(r".*\.(gpkg).zip$", name):
                return self._load_from_zip(load=gpd.read_file)
            elif re.match(r".*\.(csv).zip$", name):
                return pd.read_csv(self.filepath)
            else:
                # plain zip
                raise Exception(name)
        elif re.match(r".*\.(gz)$", name):
            if re.match(r".*\.(asc).gz$", name):
                with self.open() as file:
                    zfile = gzip.open(file)
                    return load_asciigrid(zfile)
            else:
                # plain gz
                raise Exception(name)
        elif re.match(r".*\.(bz2)$", name):
            if re.match(r".*\.(nc).bz2$", name):
                with self.open() as file:
                    zfile = bz2.open(file)
                    return xr.load_dataset(zfile)
            else:
                # plain bz2
                raise Exception(name)
        elif re.match(r".*\.(json)$", name):
            with self.open() as file:
                return json.load(file)
        elif re.match(r".*\.(txt|md|rst)$", name):
            with self.open() as file:
                bdata = file.read()
            return bdata.decode(encoding=None)
        elif re.match(r".*\.(html)$", name):
            with self.open() as file:
                return BeautifulSoup(file)
        elif re.match(r".*\.(csv)$", name):
            with self.open() as file:
                return pd.read_csv(file)
        elif re.match(r".*\.(xls|xlsx)$", name):
            with self.open() as file:
                return pd.read_excel(sheet_name=None)
        elif re.match(r".*\.(shx|dbf|cpg)$", name):
            # find associated shp
            name_shp = re.sub("[^.]+$", "shp", self.name)
            res = self.storage.resource(name=name_shp)
            return gpd.read_file(res.filepath)
        elif re.match(r".*\.(shp)$", name):
            return gpd.read_file(self.filepath)
        elif re.match(r".*\.(geojson|gpkg)$", name):
            with self.open() as file:
                return gpd.read_file(file)
        elif re.match(r".*\.(png)$", name):
            with self.open() as file:
                return xr.load_dataarray(file)
        elif re.match(r".*\.(tif|tiff)$", name):
            with self.open() as file:
                return xr.load_dataarray(file)
        elif re.match(r".*\.(nc|nc4)$", name):
            # xarray
            with self.open() as file:
                return xr.load_dataset(file)
        elif re.match(r".*\.(asc)$", name):
            # ascii grid
            with self.open() as file:
                return load_asciigrid(file)
        elif re.match(r".*\.(xyz)$", name):
            # point coords
            with self.open() as file:
                df = pd.read_csv(
                    file,
                    header=None,
                    sep=" ",
                    names=["x", "y", "z"],
                    dtype={"x": int, "y": int, "z": float},
                )
            ds = df.set_index(["x", "y"])["z"]
            ds = xr.DataArray.from_series(ds)
            print(ds)
            return ds

        elif re.match(r".*\.(prj)$", name):
            # projection string
            with self.open() as file:
                wkt = file.read().decode(encodinf="ascii")
                return pyproj.CRS.from_wkt(wkt)
        elif re.match(r".*\.(tfw)$", name):
            # projection string for tif (text)
            raise Exception(name)
        elif re.match(r".*\.(qgz|pdf|docx|woff|woff2|ipynb|py|js|css)$", name):
            raise Exception(name)
        else:
            raise Exception(name)
