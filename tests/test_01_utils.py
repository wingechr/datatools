# import logging
import os
import re
import unittest
from pathlib import PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory

from datatools.utils import (
    ByteReaderIterator,
    filepath_abs_to_uri,
    get_hostname,
    get_now_str,
    get_user_w_host,
    make_file_readonly,
    make_file_writable,
    normalize_path,
    normalize_sql_query,
    parse_cli_metadata,
    platform_is_windows,
    uri_to_filepath_abs,
)

from . import b_hello, b_hello_world, md5_hello, md5_hello_world, objects_euqal


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("/my/path//", "my/path"),
            ("c:/my/path//", "c/my/path"),
            ("file://a/b/", "file/a/b"),
            ("file://a/b#x", "file/a/bx"),
            ("https://a/b/", "http/a/b"),
            ("https://a:8000/b", "http/a/b"),
            ("https://a/b/?a=1#x", "http/a/b/x"),
            ("https://a/b?a=1#.x", "http/a/b.x"),
            ("http://a.com#x", "http/a.comx"),
            ("http://a.com#/x", "http/a.com/x"),
            ("http://user:pass@a.com", "http/a.com"),
            ("Lower  Case with SPACE ! ", "lower_case_with_space"),
            (
                "François fährt Straßenbahn zum Café Málaga",
                "francois_faehrt_strassenbahn_zum_cafe_malaga",
            ),
            (
                "https://www.domain-name.de/path%202021.pdf",
                "http/www.domain-name.de/path_2021.pdf",
            ),
        ]:
            np = normalize_path(p)
            self.assertEqual(exp_np, np, p)
            # also: normalized path should always normalize to self
            np = normalize_path(exp_np)
            self.assertEqual(exp_np, np, exp_np)

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


class TestByteReaderIterator(unittest.TestCase):
    def test_byte_reader_1(self):
        # read all at once
        bri = ByteReaderIterator(b_hello_world, hash_method="md5")
        self.assertEqual(bri.read(), b_hello_world)
        self.assertEqual(bri.hashsum(), md5_hello_world)

        # read in chunks
        bri = ByteReaderIterator(b_hello_world, chunk_size=2, hash_method="md5")
        self.assertEqual(b"".join(x for x in bri), b_hello_world)
        self.assertEqual(bri.hashsum(), md5_hello_world)

        # read truncated
        bri = ByteReaderIterator(
            b_hello_world,
            chunk_size=2,
            hash_method="md5",
            max_bytes=len(b_hello),
        )
        self.assertEqual(b"".join(x for x in bri), b_hello)
        self.assertEqual(bri.hashsum(), md5_hello)
