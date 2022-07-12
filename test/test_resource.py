import os
from functools import partial
from tempfile import TemporaryDirectory
from test import TestCase

from datatools.resource import FileResource, resource
from datatools.utils.json import dumpb
from datatools.utils.temp import NamedClosedTemporaryFile


class TestResource(TestCase):
    def test_read(self):
        res = resource(__file__)
        bytes = res.read()
        self.assertEqual(len(bytes), os.path.getsize(__file__))

        res = resource("http://example.com")
        text = res.read()
        self.assertTrue(len(text) > 0)

        sql = "select 1 as one, null as na"

        # in memory
        res = resource(f"sqlite://?sql={sql}")
        data = res.read(as_json=True)
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

        # in file (absolute path)
        with NamedClosedTemporaryFile(suffix=".sqlite3") as tempfilepath:
            res = resource(f"sqlite:///{tempfilepath}?sql={sql}")
            data = res.read(as_json=True)
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

        # in file (absolute path)
        with NamedClosedTemporaryFile(suffix=".sqlite3", dir=".") as tempfilepath:
            res = resource(f"sqlite:///{tempfilepath}?sql={sql}")
            data = res.read(as_json=True)
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

    def test_write(self):
        with NamedClosedTemporaryFile() as tempfilepath:
            res_src = resource(__file__)
            res_tgt = resource(tempfilepath)
            self.assertRaises(
                FileExistsError, partial(res_tgt.write, b"", overwrite=False)
            )
            res_tgt.write(res_src, overwrite=True)
            self.assertEqual(
                os.path.getsize(__file__),
                os.path.getsize(tempfilepath),
            )

        with NamedClosedTemporaryFile(suffix=".sqlite3", dir=".") as tempfilepath:
            data_in = [{"i": 1, "s": "s1"}, {"s": None, "i": 2}]
            res = resource(f"sqlite:///{tempfilepath}?table=test")
            report = res.write(data_in, overwrite=False)
            self.assertEqual(
                report["hash"],
                "sha256:deaa5af2ea765ed64dc21cbd06baf69462c5d5ae7818fac0966a52e77bea7aff",  # noqa
            )
            self.assertRaises(Exception, partial(res.write, data_in, overwrite=False))
            data_out = res.read(as_json=True)
            self.assertEqual(dumpb(data_in), dumpb(data_out))

    def test_dpg(self):
        data_in = [{"i": 1, "s": "s1"}, {"s": None, "i": 2}]
        with TemporaryDirectory() as tempdir:
            res = resource(tempdir + "#test.json")
            report = res.write(data_in)
            self.assertTrue(os.path.isfile(tempdir + "/datapackage.json"))
            self.assertEqual(
                report["hash"],
                "sha256:deaa5af2ea765ed64dc21cbd06baf69462c5d5ae7818fac0966a52e77bea7aff",  # noqa
            )

            # unchecked overwrite fails
            self.assertRaises(Exception, res.write, data_in)
            res.write(data_in, overwrite=True)  # works now

            report = resource(tempdir + "#test2.json").write(data_in)
            self.assertTrue(os.path.isfile(tempdir + "/data/test2.json"))
            # same hash, but different name (only data is hashed)
            self.assertEqual(
                report["hash"],
                "sha256:deaa5af2ea765ed64dc21cbd06baf69462c5d5ae7818fac0966a52e77bea7aff",  # noqa
            )

            # check if global hash is updated
            hash = FileResource(tempdir + "/datapackage.json").read(as_json=True)[
                "hash"
            ]
            self.assertEqual(
                hash,
                "sha256:0730b410d31df039f3c284ea0ac28cd27eb417bf13d078c98e854a8cf008c519",  # noqa
            )

            # load dp
            data = resource(tempdir + "#test.json").read()
            self.assertEqual(data, dumpb(data_in))
