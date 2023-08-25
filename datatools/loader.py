import bz2
import gzip
import json
import logging
import pickle
import re
import zipfile
from io import BufferedReader, BytesIO
from typing import Tuple, Union
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import requests
import sqlalchemy as sa

from .constants import DEFAULT_MEDIA_TYPE, PARAM_SQL_QUERY
from .utils import (
    get_df_table_schema,
    get_sql_table_schema,
    normalize_name,
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


def _load_from_zip(filepath, load, kwargs):
    assumed_filename_in_zip = filepath.split("/")[-1]
    with open(filepath, "rb") as file:
        zfile = zipfile.ZipFile(file)
        # find
        filename_in_zip = None
        for zf in zfile.filelist:
            logging.debug(zf.filename)
            if normalize_name(zf.filename) + ".zip" == assumed_filename_in_zip:
                filename_in_zip = zf.filename
                break
        if not filename_in_zip:
            raise Exception(f"cannot find file in zip: {filepath}")
        file_in_zip = zfile.open(filename_in_zip)
        return load(file_in_zip, **kwargs)


def _load_xyz(buf):
    if xarray is None:
        raise ImportError("xarray")
    df = pd.read_csv(
        buf,
        header=None,
        sep=" ",
        names=["x", "y", "z"],
        dtype={"x": int, "y": int, "z": float},
    )
    ds = df.set_index(["x", "y"])["z"]
    ds = xarray.DataArray.from_series(ds)
    return ds


def _load_asciigrid(buf):
    bdata = buf.read()
    sdata = bdata.decode(encoding="ascii")
    lines = sdata.splitlines()
    ascii_grid = np.loadtxt(lines, skiprows=6)
    # metadata
    for ln in lines[:6]:
        k, v = re.match("^([^ ]+)[ ]+([^ ]+)$", ln).groups()
        ascii_grid.dtype.metadata[k] = float(v)
    return ascii_grid


def _load_geopandas(source: Union[str, BufferedReader], **kwargs):
    if geopandas is None:
        raise ImportError("geopandas")
    return geopandas.read_file(source, **kwargs)


def _load_xarray(source: Union[str, BufferedReader], **kwargs):
    if xarray is None:
        raise ImportError("xarray")
    return xarray.load_dataarray(source, **kwargs)


def _load_beautifulsoup(source: Union[str, BufferedReader], **kwargs):
    if bs4 is None:
        raise ImportError("bs4")
    return bs4.BeautifulSoup(source, features="lxml", **kwargs)


def load(filepath: str, **kwargs):
    if re.match(r".*\.(zip)$", filepath):
        if re.match(r".*\.(shp).zip$", filepath):
            return _load_geopandas(filepath, **kwargs)
        elif re.match(r".*\.(gpkg).zip$", filepath):
            return _load_from_zip(load=_load_geopandas, kwargs=kwargs)
        elif re.match(r".*\.(csv).zip$", filepath):
            return pd.read_csv(filepath, **kwargs)
        else:
            # plain zip
            raise Exception(filepath)
    elif re.match(r".*\.(gz)$", filepath):
        if re.match(r".*\.(asc).gz$", filepath):
            with open(filepath, "rb") as file:
                zfile = gzip.open(file)
                return _load_asciigrid(zfile, **kwargs)
        else:
            # plain gz
            raise Exception(filepath)
    elif re.match(r".*\.(bz2)$", filepath):
        if re.match(r".*\.(nc).bz2$", filepath):
            with open(filepath, "rb") as file:
                zfile = bz2.open(file)
                return _load_xarray(zfile, **kwargs)
        else:
            # plain bz2
            raise Exception(filepath)
    elif re.match(r".*\.(json)$", filepath):
        with open(filepath, "rb") as file:
            return json.load(file, **kwargs)
    elif re.match(r".*\.(txt|md|rst)$", filepath):
        with open(filepath, "rb") as file:
            bdata = file.read()
        return bdata.decode(encoding=None, **kwargs)
    elif re.match(r".*\.(html)$", filepath):
        with open(filepath, "rb") as file:
            return _load_beautifulsoup(file, **kwargs)
    elif re.match(r".*\.(csv)$", filepath):
        with open(filepath, "rb") as file:
            return pd.read_csv(file, **kwargs)
    elif re.match(r".*\.(xls|xlsx)$", filepath):
        with open(filepath, "rb") as file:
            return pd.read_excel(sheet_name=None, **kwargs)
    elif re.match(r".*\.(shx|dbf|cpg|prj)$", filepath):
        # find associated shp
        filepath_shp = re.sub("[^.]+$", "shp", filepath)
        return _load_geopandas(filepath_shp, **kwargs)
    elif re.match(r".*\.(geojson|gpkg|shp)$", filepath):
        with open(filepath, "rb") as file:
            return _load_geopandas(file, **kwargs)
    elif re.match(r".*\.(png)$", filepath):
        with open(filepath, "rb") as file:
            return _load_xarray(file, **kwargs)
    elif re.match(r".*\.(tif|tiff)$", filepath):
        with open(filepath, "rb") as file:
            return _load_xarray(file, **kwargs)
    elif re.match(r".*\.(nc|nc4)$", filepath):
        # xarray
        with open(filepath, "rb") as file:
            return _load_xarray(file, **kwargs)
    elif re.match(r".*\.(asc)$", filepath):
        # ascii grid
        with open(filepath, "rb") as file:
            return _load_asciigrid(file, **kwargs)
    elif re.match(r".*\.(xyz)$", filepath):
        # point coords
        with open(filepath, "rb") as file:
            return _load_xyz(file, **kwargs)
    elif re.match(r".*\.(pkl|pickle)$", filepath):
        # point coords
        with open(filepath, "rb") as file:
            return pickle.load(file, **kwargs)
    elif re.match(r".*\.(tfw)$", filepath):
        # projection string for tif (text)
        raise Exception(filepath)
    elif re.match(r".*\.(qgz|pdf|docx|woff|woff2|ipynb|py|js|css)$", filepath):
        raise Exception(filepath)
    else:
        raise Exception(filepath)


def get_metadata(obj, **kwargs):
    metadata = {}
    if isinstance(obj, pd.DataFrame):
        metadata["schema"] = get_df_table_schema(obj)
        metadata["shape"] = obj.shape
    if geopandas is not None and isinstance(obj, geopandas.GeoDataFrame):
        metadata["crs"] = obj.crs.to_wkt()
        metadata["bounds"] = list(obj.total_bounds)

    return metadata


def load_uri(uri) -> Tuple[BufferedReader, dict]:
    metadata = {}
    metadata["source.path"] = remove_auth_from_uri_or_path(uri)
    url = urlsplit(uri)

    # protocol routing
    if url.scheme == "file":
        file_path = uri_to_filepath_abs(uri)
        logging.debug(f"OPEN: {file_path}")
        data = open(file_path, "rb")

    elif url.scheme in ["http", "https"]:
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
            _meta = parse_content_type(content_type)
            metadata.update(_meta)
            logging.info(_meta)

        data = res.raw

    elif "sql" in url.scheme:
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

        data = pickle.dumps(data)
        data = BytesIO(data)

        metadata["schema"] = data_schema
        metadata["mediatype"] = DEFAULT_MEDIA_TYPE

    else:
        raise NotImplementedError(url.scheme)

    return data, metadata
