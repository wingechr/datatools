import bz2
import gzip
import json
import logging
import re
import zipfile

import geopandas as gpd
import pandas as pd
import pyproj
import xarray as xr
from bs4 import BeautifulSoup

from .utils import get_df_table_schema, load_asciigrid, load_xyz, normalize_name


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


def load(filepath: str, **kwargs):
    if re.match(r".*\.(zip)$", filepath):
        if re.match(r".*\.(shp).zip$", filepath):
            return gpd.read_file(filepath, **kwargs)
        elif re.match(r".*\.(gpkg).zip$", filepath):
            return _load_from_zip(load=gpd.read_file, kwargs=kwargs)
        elif re.match(r".*\.(csv).zip$", filepath):
            return pd.read_csv(filepath, **kwargs)
        else:
            # plain zip
            raise Exception(filepath)
    elif re.match(r".*\.(gz)$", filepath):
        if re.match(r".*\.(asc).gz$", filepath):
            with open(filepath, "rb") as file:
                zfile = gzip.open(file)
                return load_asciigrid(zfile, **kwargs)
        else:
            # plain gz
            raise Exception(filepath)
    elif re.match(r".*\.(bz2)$", filepath):
        if re.match(r".*\.(nc).bz2$", filepath):
            with open(filepath, "rb") as file:
                zfile = bz2.open(file)
                return xr.load_dataset(zfile, **kwargs)
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
            return BeautifulSoup(file, features="lxml", **kwargs)
    elif re.match(r".*\.(csv)$", filepath):
        with open(filepath, "rb") as file:
            return pd.read_csv(file, **kwargs)
    elif re.match(r".*\.(xls|xlsx)$", filepath):
        with open(filepath, "rb") as file:
            return pd.read_excel(sheet_name=None, **kwargs)
    elif re.match(r".*\.(shx|dbf|cpg)$", filepath):
        # find associated shp
        filepath_shp = re.sub("[^.]+$", "shp", filepath)
        return gpd.read_file(filepath_shp, **kwargs)
    elif re.match(r".*\.(shp)$", filepath):
        return gpd.read_file(filepath)
    elif re.match(r".*\.(geojson|gpkg)$", filepath):
        with open(filepath, "rb") as file:
            return gpd.read_file(file, **kwargs)
    elif re.match(r".*\.(png)$", filepath):
        with open(filepath, "rb") as file:
            return xr.load_dataarray(file, **kwargs)
    elif re.match(r".*\.(tif|tiff)$", filepath):
        with open(filepath, "rb") as file:
            return xr.load_dataarray(file, **kwargs)
    elif re.match(r".*\.(nc|nc4)$", filepath):
        # xarray
        with open(filepath, "rb") as file:
            return xr.load_dataset(file, **kwargs)
    elif re.match(r".*\.(asc)$", filepath):
        # ascii grid
        with open(filepath, "rb") as file:
            return load_asciigrid(file, **kwargs)
    elif re.match(r".*\.(xyz)$", filepath):
        # point coords
        with open(filepath, "rb") as file:
            return load_xyz(file, **kwargs)
    elif re.match(r".*\.(prj)$", filepath):
        # projection string
        with open(filepath, "rb") as file:
            wkt = file.read().decode(encodinf="ascii")
            return pyproj.CRS.from_wkt(wkt)
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
    if isinstance(obj, gpd.GeoDataFrame):
        metadata["crs"] = obj.crs
        metadata["bounds"] = list(obj.total_bounds)

    return metadata
