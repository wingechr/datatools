# import logging
import inspect
import os
import re
import unittest
from functools import partial
from io import BytesIO
from pathlib import PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import datatools
from datatools.utils import (
    CsvSerializer,
    as_byte_iterator,
    df_to_values,
    filepath_abs_to_uri,
    get_df_table_schema,
    get_function_info,
    get_hostname,
    get_now_str,
    get_resource_path_name,
    get_sql_uri,
    get_sqlite_query_uri,
    get_suffix,
    get_user_w_host,
    is_callable,
    is_file_readonly,
    is_uri,
    json_dumps,
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

        # test various null types
        for x in [None, inspect._empty, np.nan, pd.NA]:
            self.assertEqual(json_dumps(x), "null")

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
        sql_query = "select a, b from t"
        self.assertEqual(
            get_sql_uri(
                connection_string_uri="mssql+pyodbc://"
                "?odbc_connect=driver=sql server;server=myserver",
                sql_query=sql_query,
            ),
            "mssql+pyodbc://?odbc_connect=driver=sql server;server=myserver&q="
            "SELECT%20a%2C%20b%20FROM%20t",
        )
        self.assertEqual(
            get_sqlite_query_uri(location=None, sql_query=sql_query),  # :memory:,
            "sqlite:///:memory:?q=SELECT%20a%2C%20b%20FROM%20t",
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

    def test_get_function_info(self):
        """get information from different types of callables.
        callables can be
        - functions
        - partials
        - decorated functions (with closure)
        - lambda expression

        """
        b = 100

        # regular function
        def myfunc(a, c=1, d=None):
            "myfunc docstring"
            # b is from outer context
            return a + b + c + (d or 0)

        # function with partially bound args
        myfunc_partial = partial(myfunc, 5, d=20)

        # decorated function (with closure)
        def make_myfunc_closure(b=None):
            a = 10

            def decorator(myfunc):

                def myfunc_closure(c, d):
                    return myfunc(a=a, b=b, c=c, d=d)

                myfunc_closure.__name__ = myfunc.__name__
                myfunc_closure.__doc__ = myfunc.__doc__

                return myfunc_closure

            return decorator

        _myfunc_closure = make_myfunc_closure(5)(myfunc)

        # lambda expression
        f_lambda = lambda a, b, c, d: a + b + c + d  # noqa
        f_lambda.__name__ = myfunc.__name__
        f_lambda.__doc__ = myfunc.__doc__

        info = get_function_info(myfunc)
        info_partial = get_function_info(myfunc_partial)
        info_closure = get_function_info(_myfunc_closure)
        info_lambda = get_function_info(f_lambda)

        # check generated names
        self.assertEqual(info["name"], "myfunc")
        # partial: name of original function
        self.assertEqual(info_partial["name"], "myfunc")
        # decorator: inner name (from inside decorator)
        # but: in this example, we set myfunc_closure.__name__ = myfunc.__name__
        self.assertEqual(info_closure["name"], "myfunc")
        self.assertEqual(info_lambda["name"], "myfunc")  # manually set

        # get docstrings
        self.assertEqual(info["doc"], "myfunc docstring")
        self.assertEqual(info_partial["doc"], "myfunc docstring")
        # decorator: from inside decorator
        # but: in this example, we set myfunc_closure.__doc__ = myfunc.__doc__
        self.assertEqual(info_closure["doc"], "myfunc docstring")
        self.assertEqual(info_lambda["doc"], "myfunc docstring")  # manually set

        # get file in which function is defined
        this_file = os.path.realpath(__file__)
        self.assertEqual(os.path.realpath(info["file"]), this_file)
        self.assertEqual(os.path.realpath(info_partial["file"]), this_file)
        self.assertEqual(os.path.realpath(info_closure["file"]), this_file)
        self.assertEqual(os.path.realpath(info_lambda["file"]), this_file)

        # if possible: get bound values
        # normal function: only get default values
        self.assertDictEqual(
            info["kwargs"],
            {
                "a": None,
                "b": 100,  # from outer context
                "c": 1,  # default value
                "d": None,
            },
        )
        # partial: get all bound
        self.assertDictEqual(
            info_partial["kwargs"],
            {
                "a": 5,  # from partial
                "b": 100,  # from outer context
                "c": 1,  # default value
                "d": 20,  # from partial
            },
        )
        # decorator/closure:
        self.assertDictEqual(
            info_closure["kwargs"],
            {
                "a": 10,  # from within decorator
                "b": 5,  # from decorator call
                "c": None,
                "d": None,
            },
        )
        # lmabda: nothing bound
        self.assertDictEqual(
            info_lambda["kwargs"], {"a": None, "b": None, "c": None, "d": None}
        )

        info2 = get_function_info(is_uri)

        # version
        self.assertEqual(info2["version"], f"datatools {datatools.__version__}")

        # git
        git_info = info2["git"]
        self.assertTrue(git_info["origin"])

    def test_is_callable(self):

        # regular function
        def f_fun(x):
            return x

        # lambda
        f_lambda = lambda x: x  # noqa

        # callable class
        class F:
            def __call__(self, x):
                return x

        f_cls = F()

        # partial

        def _f_fun(y, x):
            return x

        f_partial = partial(_f_fun, "Y")

        # closure

        def _f_closure(y):
            def f_closure(x):
                return _f_fun(y, x)

            return f_closure

        f_closure = _f_closure("y")

        f_builtin = int

        for callable in [f_builtin, f_fun, f_lambda, f_cls, f_partial, f_closure]:
            self.assertTrue(is_callable(callable), callable)
            self.assertEqual(callable(999), 999, callable)

        for not_callable in ["string", 10]:
            self.assertFalse(is_callable(not_callable), not_callable)

    def test_get_function_info_should_not_change_function(self):
        def example_fun(a, b, c=1):
            return a + b + c

        example_partial = partial(example_fun, 4, b=2)

        args1, kwargs1 = example_partial.args, example_partial.keywords
        get_function_info(example_partial)
        args2, kwargs2 = example_partial.args, example_partial.keywords
        # args and kwargs have not changed
        self.assertEqual(args1, args2)
        self.assertEqual(kwargs1, kwargs2)


class TestSerializers(unittest.TestCase):
    def test_CsvSerializer(self):
        serializer = CsvSerializer()
        data = {"a": [1, 2], "b": [3, 4]}
        bdata = bytes(serializer.dumps(data))
        self.assertEqual(bdata, b"a,b\n1,3\n2,4\n")
