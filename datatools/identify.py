import numpy as np
import pandas as pd

from .utils import get_df_table_schema

try:
    import geopandas
except ImportError:
    geopandas = None


def guess_metadata(obj, **kwargs):
    metadata = {}
    if isinstance(obj, pd.DataFrame):
        metadata["schema"] = get_df_table_schema(obj)
        metadata["shape"] = obj.shape
    if geopandas is not None and isinstance(obj, geopandas.GeoDataFrame):
        metadata["crs"] = obj.crs.to_wkt()
        metadata["bounds"] = list(obj.total_bounds)

    return metadata
