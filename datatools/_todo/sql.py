from urllib.parse import parse_qs, urlparse, urlunparse

import sqlalchemy as sa

from .files import assert_not_exist
from .json import json_dump


def download_sql(source_uri, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)
    source_uri = urlparse(source_uri)
    query = parse_qs(source_uri.query)["q"][0]
    source_uri = urlunparse(source_uri._replace(query=None))
    eng = sa.create_engine(source_uri)
    cur = eng.execute(query)
    data = []
    for row in cur.fetchall():
        row = dict(row)
        data.append(row)
    ext = target_file_path.split(".")[-1]
    if ext == "json":
        json_dump(data, target_file_path)
    else:
        raise NotImplementedError(ext)
