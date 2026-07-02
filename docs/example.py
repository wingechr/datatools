"""Example script."""
# ruff: noqa: S101, D103

from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from datatools import FileDataStorage
from datatools.utils import start_http_server
from tests.test_storage import QueryParameterUri

# test http server
this_file_path = Path(__file__)
test_url = start_http_server(directory=this_file_path.parent, port=8000)

with TemporaryDirectory() as tempdir:
    # create storage
    st = FileDataStorage(tempdir)

    # import http (import this file)
    uri1 = test_url + "/" + this_file_path.name
    name1 = st.import_from_uri(uri1)
    assert name1 == "127.0.0.1/example.py", name1  # noqa: S101

    # import this file again, but from path
    uri2 = this_file_path.as_uri()  # not really uri, but still works
    name2 = st.import_from_uri(uri2)
    assert name2 == "example.py", name2

    # import from sql
    uri3 = "sqlite:///:memory:"
    query = "select 1 as value"
    name3 = st.import_from_uri(uri3, name="result.csv", query=query)
    assert name3 == "result.csv", name3

    # create some functions
    def load_csv(data: bytes) -> pd.DataFrame:
        return pd.read_csv(BytesIO(data))

    def sum_values(value1: int, value2: int, value3: pd.DataFrame) -> int:
        """sum of input values"""
        return value1 + value2 + int(value3["value"].sum())

    # define a job
    job = st.job(
        sum_values,
        # to / from bytes convert fro output/input
        output_converters=lambda x: json.dumps(x).encode(),
        input_converters={
            "value1": len,
            "value2": len,
            "value3": load_csv,
        },
        skip_finished=True,
    )

    # run job
    name4 = "result.json"
    job(name4, value1=name1, value2=name2, value3=name3)

    # check result and metadata
    assert len(st[name1]) == len(st[name2])  # imported same files 2 times
    assert json.loads(st[name4]) == 2 * len(st[name1]) + 1

    assert st.metadata(name1)[QueryParameterUri][0] == uri1
    assert st.metadata(name2)[QueryParameterUri][0] == uri2
    assert st.metadata(name3)[QueryParameterUri][0] == uri3
