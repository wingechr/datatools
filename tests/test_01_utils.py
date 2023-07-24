# coding: utf-8
import logging
import os
import re
import unittest
from pathlib import PurePosixPath, PureWindowsPath
from tempfile import TemporaryDirectory

from datatools.utils import (
    file_uri_to_path,
    get_hostname,
    get_now_str,
    get_user_w_host,
    make_file_readonly,
    make_file_writable,
    normalize_path,
    parse_cli_metadata,
    path_to_file_uri,
    platform_is_windows,
    uri_to_data_path,
)

from . import objects_euqal

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("/my/path//", "my/path"),
            ("c:/my/path//", "c/my/path"),
            ("file://a/b/", "file/a/b"),
            ("file://a/b#x", "file/a/bx"),
            ("Lower  Case with SPACE ! ", "lower_case_with_space"),
            (
                "François fährt Straßenbahn zum Café Málaga",
                "francois_faehrt_strassenbahn_zum_cafe_malaga",
            ),
        ]:
            np = normalize_path(p)
            self.assertEqual(exp_np, np)
            # also: normalized path should always normalize to self
            np = normalize_path(exp_np)
            self.assertEqual(exp_np, np)

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
        for file_path, uri in examples:
            self.assertEqual(uri, path_to_file_uri(file_path))
            self.assertEqual(str(file_path), file_uri_to_path(uri))

        for data_path, uri in [
            ("http/a/b", "https://a/b/"),
            ("http/a/b", "https://a:8000/b"),
            ("http/a/b/x", "https://a/b/?a=1#x"),
            ("http/a/b.x", "https://a/b?a=1#.x"),
            ("http/a.comx", "http://a.com#x"),
            ("http/a.com/x", "http://a.com#/x"),
            ("http/a.com", "http://user:pass@a.com"),
        ]:
            self.assertEqual(data_path, uri_to_data_path(uri))

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
