# coding: utf-8
import logging
import unittest
from pathlib import PurePosixPath, PureWindowsPath

from datatools.utils import (
    file_uri_to_path,
    get_hostname,
    normalize_path,
    path_to_file_uri,
    uri_to_data_path,
)

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("/my/path//", "my/path"),
            ("c:/my/path//", "c/my/path"),
            ("file://a/b/", "file/a/b"),
            ("file://a/b#x", "file/a/b#x"),
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
        ]:
            self.assertEqual(data_path, uri_to_data_path(uri))
