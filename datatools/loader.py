import bz2
import gzip
import json
import logging
import re
import zipfile
from io import BufferedReader
from typing import Union

import numpy as np
import pandas as pd

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


from .utils import get_df_table_schema, normalize_name


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
