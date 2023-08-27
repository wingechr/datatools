import bz2
import gzip
import json
import logging
import pickle
import re
import zipfile
from io import BytesIO
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa

from .constants import PARAM_SQL_QUERY
from .utils import (
    PickleSerializer,
    get_df_table_schema,
    get_resource_path_name,
    get_sql_table_schema,
    normalize_sql_query,
    parse_content_type,
    remove_auth_from_uri_or_path,
    uri_to_filepath_abs,
)

# optional
try:
    import geopandas
except ImportError:
    geopandas = None

# optional
try:
    import xarray
except ImportError:
    xarray = None

# optional
try:
    import bs4
except ImportError:
    bs4 = None


def _TODO_get_metadata(obj, **kwargs):
    metadata = {}
    if isinstance(obj, pd.DataFrame):
        metadata["schema"] = get_df_table_schema(obj)
        metadata["shape"] = obj.shape
    if geopandas is not None and isinstance(obj, geopandas.GeoDataFrame):
        metadata["crs"] = obj.crs.to_wkt()
        metadata["bounds"] = list(obj.total_bounds)

    return metadata


class FileLoaderMeta(type):
    _pattern_classes = []

    def __init__(cls, name, bases, dct):
        if cls.filename_pattern:
            cls._pattern_classes.append((cls.filename_pattern, cls))


class FileLoader(metaclass=FileLoaderMeta):
    filename_pattern = None

    @classmethod
    def load(cls, filepath, **kwargs):
        for pat, class_ in cls._pattern_classes:
            if re.match(pat, filepath):
                inst = class_()
                return inst.load(filepath, **kwargs)
        raise NotImplementedError(filepath)


class FileLoaderOpen_(metaclass=FileLoaderMeta):
    filename_pattern = None

    def load(self, filepath, **kwargs):
        with open(filepath, "rb") as file:
            return self._load(file=file, **kwargs)

    def _load(self, file, **kwargs):
        raise NotImplementedError()


class FileLoaderOpenGz_(FileLoaderOpen_):
    def load(self, filepath, **kwargs):
        with super().open(filepath, "rb") as file:
            zfile = gzip.open(file)
            return self._load(file=zfile, **kwargs)


class FileLoaderOpenBz2_(FileLoaderOpen_):
    def load(self, filepath, **kwargs):
        with super().open(filepath, "rb") as file:
            zfile = bz2.open(file)
            return self._load(file=zfile, **kwargs)


class FileLoaderOpenZip_(FileLoader):
    def load(self, filepath, **kwargs):
        assumed_filename_in_zip = filepath.split("/")[-1]
        with open(filepath, "rb") as file:
            zfile = zipfile.ZipFile(file)
            # find
            filename_in_zip = None
            for zf in zfile.filelist:
                zf_norm_name = get_resource_path_name(zf.filename.lower + ".zip")
                if zf_norm_name == assumed_filename_in_zip:
                    filename_in_zip = zf.filename
                    break
            if not filename_in_zip:
                raise Exception(f"cannot find file in zip: {filepath}")
            file_in_zip = zfile.open(filename_in_zip)
            return self._load(file=file_in_zip, **kwargs)


class FileLoaderPickle(FileLoaderOpen_):
    filename_pattern = r".*\.(pkl|pickle)$"

    def _load(self, file, **kwargs):
        return pickle.load(file, **kwargs)


class FileLoaderXarray(FileLoaderOpen_):
    filename_pattern = r".*\.(tif|tiff|png|nc|nc4)$"

    def _load(self, file, **kwargs):
        if xarray is None:
            raise ImportError("xarray")
        return xarray.load_dataarray(file, **kwargs)


class FileLoaderXYZ(FileLoaderOpen_):
    filename_pattern = r".*\.(xyz)$"

    def _load(self, file, **kwargs):
        if xarray is None:
            raise ImportError("xarray")
        df = pd.read_csv(
            file,
            header=None,
            sep=" ",
            names=["x", "y", "z"],
            dtype={"x": int, "y": int, "z": float},
        )
        ds = df.set_index(["x", "y"])["z"]
        ds = xarray.DataArray.from_series(ds)
        return ds


class FileLoaderAsciigrid(FileLoaderOpen_):
    filename_pattern = r".*\.(asc)$"

    def _load(self, file, **kwargs):
        bdata = file.read()
        sdata = bdata.decode(encoding="ascii")
        lines = sdata.splitlines()
        ascii_grid = np.loadtxt(lines, skiprows=6)
        # metadata
        for ln in lines[:6]:
            k, v = re.match("^([^ ]+)[ ]+([^ ]+)$", ln).groups()
            ascii_grid.dtype.metadata[k] = float(v)
        return ascii_grid


class FileLoaderJson(FileLoaderOpen_):
    filename_pattern = r".*\.(json)$"

    def _load(self, file, **kwargs):
        return json.load(file, **kwargs)


class FileLoaderText(FileLoaderOpen_):
    filename_pattern = r".*\.(txt|md|rst)$"

    def _load(self, file, **kwargs):
        return file.read().decode(encoding=None, **kwargs)


class FileLoaderHtml(FileLoaderOpen_):
    filename_pattern = r".*\.(html)$"

    def _load(self, file, **kwargs):
        if bs4 is None:
            raise ImportError("bs4")
        return bs4.BeautifulSoup(file, features="html.parser", **kwargs)


class FileLoaderCsv(FileLoaderOpen_):
    filename_pattern = r".*\.(csv)$"

    def _load(self, file, **kwargs):
        return pd.read_csv(file, **kwargs)


class FileLoaderExcel(FileLoaderOpen_):
    filename_pattern = r".*\.(xls|xlsx)$"

    def _load(self, file, **kwargs):
        return pd.read_excel(sheet_name=None, **kwargs)


class FileLoaderGeo(FileLoaderOpen_):
    filename_pattern = r".*\.(geojson|gpkg|shp)$"

    def _load(self, file, **kwargs):
        if geopandas is None:
            raise ImportError("geopandas")
        return geopandas.read_file(file, **kwargs)


class FileLoaderXarrayBz2(FileLoaderOpenBz2_):
    filename_pattern = r".*\.(nc).bz2$"

    def _load(self, file, **kwargs):
        return FileLoaderXarray(file, **kwargs)


class FileLoaderAsciigridGz(FileLoaderOpenGz_):
    filename_pattern = r".*\.(asc).gz$"

    def _load(self, file, **kwargs):
        return FileLoaderAsciigrid()._load(file, **kwargs)


class FileLoaderShapefile(FileLoader):
    filename_pattern = r".*\.(shx|dbf|cpg|prj)$"

    def _load(self, filepath, **kwargs):
        filepath_shp = re.sub("[^.]+$", "shp", filepath)
        if geopandas is None:
            raise ImportError("geopandas")
        return geopandas.read_file(filepath_shp, **kwargs)


class FileLoaderShapefileZip(FileLoaderShapefile):
    filename_pattern = r".*\.(shp).zip$"


class FileLoaderGeoZip(FileLoaderOpenZip_):
    filename_pattern = r".*\.(gpkg).zip$"

    def _load(self, file, **kwargs):
        return FileLoaderGeo()._load(file, **kwargs)


class FileLoaderCsvZip(FileLoaderOpenZip_):
    filename_pattern = r".*\.(csv).zip"

    def _load(self, file, **kwargs):
        return FileLoaderCsv()._load(file, **kwargs)


class UriLoaderMeta(type):
    _pattern_classes = []

    def __init__(cls, name, bases, dct):
        if cls.uri_pattern:
            cls._pattern_classes.append((cls.uri_pattern, cls))


class UriLoader(metaclass=UriLoaderMeta):
    uri_pattern = None
    serializer = None

    @classmethod
    def open_data_metadata(cls, uri, **kwargs):
        for pat, class_ in cls._pattern_classes:
            if re.match(pat, uri):
                inst = class_()
                return inst.open_data_metadata(uri, **kwargs)
        raise NotImplementedError(uri)


class UriLoaderFile(UriLoader):
    uri_pattern = "^file://"

    def open_data_metadata(self, uri, **kwargs):
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(uri)
        file_path = uri_to_filepath_abs(uri)
        logging.debug(f"OPEN: {file_path}")
        data = open(file_path, "rb")

        return data, metadata


class UriLoaderHttp(UriLoader):
    uri_pattern = "^https?://"

    def open_data_metadata(self, uri, **kwargs):
        url = urlsplit(uri)
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(uri)

        # because we want to encode auth headers
        # in the uri, we place it in the netloc
        # part before the @host (instead of the basic auth)
        # if it's basic auth, the pattern is `user:pass`
        # if it's a header, it's header=value
        match_header = re.match("^([^=@]+)=([^=@]+)@(.+)$", url.netloc)
        headers = {}
        if match_header:
            h_name, h_val, netloc = match_header.groups()
            h_name = unquote(h_name)
            h_val = unquote(h_val)
            headers[h_name] = h_val
            logging.debug("Stripping auth header from uri")

        else:
            netloc = url.netloc

        # TODO: is self.query encoded properly automatically?

        url = urlunsplit([url.scheme, netloc, url.path, url.query, None])
        logging.debug(f"OPEN: {url}")
        res = requests.get(url, stream=True, headers=headers)

        res.raise_for_status()
        content_type = res.headers.get("Content-Type")

        if content_type:
            metadata.update(parse_content_type(content_type))

        data = res.raw
        return data, metadata


class UriLoaderSql(UriLoader):
    uri_pattern = r"^[\w]*sql[\w]*://"
    serializer = PickleSerializer()

    def open_data_metadata(self, uri, **kwargs):
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(uri)
        url = urlsplit(uri)

        # pop sql query
        query_dict = parse_qs(url.query)
        sql_query = query_dict.pop(PARAM_SQL_QUERY)[0]
        sql_query = unquote(sql_query)
        sql_query = normalize_sql_query(sql_query)

        metadata["source.query"] = sql_query

        # usually, netloc is empty, and so urlunsplit()
        # drops the "//"" at the beginning
        path = url.path if url.netloc else "//" + url.path
        # doseq: if False: encode arrays differently
        query_str = urlencode(query_dict, doseq=True)

        connection_string = urlunsplit([url.scheme, url.netloc, path, query_str, None])
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

        data = self.serializer.dumps(data)
        data = BytesIO(data)

        metadata["schema"] = data_schema
        metadata["mediatype"] = self.serializer.mediatype

        return data, metadata
