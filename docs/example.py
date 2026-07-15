"""Example script."""
# ruff: noqa: S101, D103

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import pandas as pd

from datatools import FileDataStorage
from datatools.utils import json_dumpb, json_loadb, start_http_server
from tests.test_storage import QueryParameterUri

if TYPE_CHECKING:
    from _typeshed import SupportsRead

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
    def load_csv(data: "SupportsRead[bytes]") -> pd.DataFrame:
        return pd.read_csv(data)  # type:ignore (ReadCsvBuffer[bytes] still works)

    def len_bytes(data: "SupportsRead[bytes]") -> int:
        return len(data.read())

    def sum_values(value1: int, value2: int, value3: pd.DataFrame) -> int:
        """sum of input values"""
        return value1 + value2 + int(value3["value"].sum())

    # define a task
    task = st.task(
        sum_values,
        # to / from bytes convert fro output/input
        output_converters=json_dumpb,
        input_converters={
            "value1": len_bytes,
            "value2": len_bytes,
            "value3": load_csv,
        },
        skip_finished=True,
    )

    # run task
    name4 = "result.json"
    task(name4, value1=name1, value2=name2, value3=name3)

    # check result and metadata
    assert len(st.read(name1)) == len(st.read(name2))  # imported same files 2 times
    assert json_loadb(st.read(name4)) == 2 * len(st.read(name1)) + 1

    assert st.metadata(name1).get(QueryParameterUri)[0] == uri1
    assert st.metadata(name2).get(QueryParameterUri)[0] == uri2
    assert st.metadata(name3).get(QueryParameterUri)[0] == uri3
