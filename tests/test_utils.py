# import logging
import os
import re
import unittest
from io import BytesIO
from pathlib import PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from datatools.utils import (
    CsvSerializer,
    as_byte_iterator,
    df_to_values,
    filepath_abs_to_uri,
    get_df_table_schema,
    get_hostname,
    get_now_str,
    get_resource_path_name,
    get_sql_uri,
    get_suffix,
    get_user_w_host,
    is_file_readonly,
    is_uri,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_sql_query,
    parse_cli_metadata,
    platform_is_windows,
    uri_to_data_path,
    uri_to_filepath_abs,
)

from . import objects_euqal


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("file://a/b/", "a/b"),
            ("file://a/b#x", "a/bx"),
            ("https://a/b/", "a/b"),
            ("https://a:8000/b", "a/b"),
            ("https://a/b/?a=1#x", "a/b/x"),
            ("https://a/b?a=1#.x", "a/b.x"),
            ("http://a.com#x", "a.comx"),
            ("http://a.com#/x", "a.com/x"),
            ("http://user:pass@a.com", "a.com"),
            ("http://header=token@a.com", "a.com"),
            ("Lower  Case with SPACE ! ", "lower_case_with_space"),
            (
                "François fährt Straßenbahn zum Café Málaga",
                "francois_faehrt_strassenbahn_zum_cafe_malaga",
            ),
            (
                "https://www.domain-name.de/path%202021.pdf",
                "www.domain-name.de/path_2021.pdf",
            ),
            ("path%202021.pdf", "path_2021.pdf"),
        ]:
            if is_uri(p):
                p = uri_to_data_path(p)
            np = get_resource_path_name(p)
            self.assertEqual(np, exp_np, p)
            # also: normalized path should always normalize to self
            np = get_resource_path_name(exp_np)
            self.assertEqual(np, exp_np, exp_np)

    def test_path_to_file_uri(self):
        host = get_hostname()
        examples = [
            (
                PurePosixPath("/path/file name.suffix"),
                f"file://{host}/path/file name.suffix",
            ),  # posix path
            (
                PureWindowsPath(r"S:\path\file name.suffix"),
                f"file://{host}/S:/path/file name.suffix",
            ),  # windows path
            (
                PureWindowsPath(r"\\UNC_HOST\path\file name.suffix"),
                "file://UNC_HOST/path/file name.suffix",
            ),  # windows unc shared
        ]
        for file_path_abs, uri in examples:
            self.assertEqual(uri, filepath_abs_to_uri(file_path_abs))
            self.assertEqual(str(file_path_abs), uri_to_filepath_abs(uri))

    def test_make_file_readonly(self):
        with TemporaryDirectory() as dir:
            # create a file
            filepath = f"{dir}/test.txt"

            with open(filepath, "wb"):
                pass

            make_file_readonly(filepath)
            self.assertTrue(is_file_readonly(filepath))

            # reading should be ok
            with open(filepath, "rb"):
                pass

            # but not writing
            self.assertRaises(PermissionError, open, filepath, "wb")

            # or deleting (only windows)
            if platform_is_windows():
                self.assertRaises(PermissionError, os.remove, filepath)

            # but we can revers it:
            make_file_writable(filepath)
            self.assertFalse(is_file_readonly(filepath))

            os.remove(filepath)

    def test_get_now_str(self):
        now_str = get_now_str()
        n2 = "[0-9]{2}"
        pattern = f"^{n2}{n2}-{n2}-{n2}T{n2}:{n2}:{n2}[+-]{n2}:{n2}$"
        self.assertTrue(re.match(pattern, now_str))

    def test_get_user_w_host(self):
        user = get_user_w_host()
        self.assertTrue(re.match(".+@.+", user))

    def test_parse_cli_metadata(self):
        self.assertTrue(
            objects_euqal(parse_cli_metadata(["a=b", "c=1"]), {"a": "b", "c": 1})
        )

    def test_normalize_sql_query(self):
        for q, eq in [
            (
                '\n select x,\n\t "y" /* comment*/   from [z];  ',
                'SELECT x, "y" FROM [z];',
            )
        ]:
            self.assertEqual(normalize_sql_query(q), eq)

    def test_as_byte_iterator(self):
        data = b"hello world"

        # check bytes
        data1 = data
        data_l = list(as_byte_iterator(data1))
        self.assertEqual(len(data_l), 1)
        self.assertEqual(b"".join(data_l), data)

        # check BufferedReader
        data1 = BytesIO(data1)
        data_l = list(as_byte_iterator(data1))
        # self.assertEqual(len(data_l), 1)
        self.assertEqual(b"".join(data_l), data)

        # check iterable
        data1 = [data[:5], data[5:]]
        data_l = list(as_byte_iterator(data1))
        self.assertEqual(len(data_l), 2)
        self.assertEqual(b"".join(data_l), data)

        # check self
        data1 = as_byte_iterator([data[:5], data[5:]])
        data_l = list(as_byte_iterator(data1))
        self.assertEqual(len(data_l), 2)
        self.assertEqual(b"".join(data_l), data)

    def test_json_serialize(self):
        res = json_serialize(object)
        self.assertEqual(res, "object")

    def test_get_df_table_schema(self):
        df = pd.DataFrame(
            {
                "i": [0, 0, 80, 10, 20],
                "c": ["x", "x", "x", None, "x"],
                "b": [True, False, True, True, False],
                "f": [np.inf, np.nan, 10.1, -12.231, 1e4],
            }
        )
        schema = get_df_table_schema(df)
        self.assertTrue(
            objects_euqal(
                schema,
                {
                    "fields": [
                        {"name": "i", "data_type": "int64", "is_nullable": False},
                        {"name": "c", "data_type": "object", "is_nullable": True},
                        {"name": "b", "data_type": "bool", "is_nullable": False},
                        {"name": "f", "data_type": "float64", "is_nullable": True},
                    ]
                },
            ),
            schema,
        )

    def test_get_sql_uri(self):
        self.assertEqual(
            get_sql_uri(
                connection_string_uri="mssql+pyodbc://"
                "?odbc_connect=driver=sql server;server=myserver",
                sql_query="select a, b from t",
            ),
            "mssql+pyodbc://?odbc_connect=driver=sql server;server=myserver&q="
            "SELECT%20a%2C%20b%20FROM%20t",
        )

    def test_get_suffix(self):
        for path, suffix in [
            ("filename.png", ".png"),
            ("filename.csv.zip", ".csv.zip"),
            ("path/filename.csv.zip", ".csv.zip"),
            ("path.path/filename.csv.zip", ".csv.zip"),
            ("path/.gitignore", ""),
            ("path/gitignore", ""),
        ]:
            self.assertEqual(get_suffix(path), suffix)

    def test_df_to_values(self):
        self.assertEqual(
            df_to_values(
                pd.DataFrame({"s": ["a", "b"], "i": [1, None], "f": [1.5, np.nan]})
            ),
            [{"f": 1.5, "i": 1.0, "s": "a"}, {"f": None, "i": None, "s": "b"}],
        )


class TestSerializers(unittest.TestCase):
    def test_CsvSerializer(self):
        serializer = CsvSerializer()
        data = {"a": [1, 2], "b": [3, 4]}
        bdata = bytes(serializer.dumps(data))
        self.assertEqual(bdata, b"a,b\n1,3\n2,4\n")
