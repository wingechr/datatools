"""Example script."""
# ruff: noqa: S101, D103

from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from datatools import FileDataStorage
from datatools.utils import start_http_server

# test http server
this_file_path = Path(__file__)
test_url = start_http_server(directory=this_file_path.parent, port=8000)

with TemporaryDirectory() as tempdir:
    # create storage
    st = FileDataStorage(tempdir)

    # import http (import this file)
    uri1 = test_url + "/" + this_file_path.name
    uid1 = st.import_from_uri(uri1)
    assert uid1 == "127.0.0.1/example.py", uid1  # noqa: S101

    # import this file again, but from path
    uri2 = this_file_path.as_uri()  # not really uri, but still works
    uid2 = st.import_from_uri(uri2)
    assert uid2 == "example.py", uid2

    # import from sql
    uri3 = "sqlite:///:memory:"
    query = "select 1 as value"
    uid3 = st.import_from_uri(uri3, uid="result.csv", query=query)
    assert uid3 == "result.csv", uid3

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
    uid4 = "result.json"
    job(uid4, value1=uid1, value2=uid2, value3=uid3)

    # check result and metadata
    assert len(st[uid1]) == len(st[uid2])  # imported same files 2 times
    assert json.loads(st[uid4]) == 2 * len(st[uid1]) + 1

    assert st.metadata(uid1)["origin.parameter.uri"][0] == uri1
    assert st.metadata(uid2)["origin.parameter.uri"][0] == uri2
    assert st.metadata(uid3)["origin.parameter.uri"][0] == uri3
    assert st.metadata(uid3)["origin.parameter.query"][0] == query
    assert st.metadata(uid4)["origin.parameter.value1.@value"][0] == uid1
    assert st.metadata(uid4)["origin.parameter.value2.@value"][0] == uid2
    assert st.metadata(uid4)["origin.parameter.value3.@value"][0] == uid3

    raise Exception(st.metadata(uid4)["origin"])
