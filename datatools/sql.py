from urllib.parse import parse_qs, urlparse, urlunparse

import sqlalchemy as sa

from .json import json_dump


def download_sql(source_uri, target_filepath):
    source_uri = urlparse(source_uri)
    query = parse_qs(source_uri.query)["q"][0]
    source_uri = urlunparse(source_uri._replace(query=None))
    eng = sa.create_engine(source_uri)
    cur = eng.execute(query)
    data = []
    for row in cur.fetchall():
        row = dict(row)
        data.append(row)
    ext = target_filepath.split(".")[-1]
    if ext == "json":
        json_dump(data, target_filepath)
    else:
        raise NotImplementedError(ext)
