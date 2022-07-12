import os
from functools import partial
from test import TestCase

from datatools.resource import resource
from datatools.utils.json import dumps
from datatools.utils.temp import NamedClosedTemporaryFile


class TestResource(TestCase):
    def test_read(self):
        res = resource(__file__)
        bytes = res.read_bytes()
        self.assertEqual(len(bytes), os.path.getsize(__file__))

        res = resource("http://example.com")
        text = res.read_text()
        self.assertTrue(len(text) > 0)

        sql = "select 1 as one, null as na"

        # in memory
        res = resource(f"sqlite://?sql={sql}")
        data = res.read_json()
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

        # in file (absolute path)
        with NamedClosedTemporaryFile(suffix=".sqlite3") as tempfilepath:
            res = resource(f"sqlite:///{tempfilepath}?sql={sql}")
            data = res.read_json()
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

        # in file (absolute path)
        with NamedClosedTemporaryFile(suffix=".sqlite3", dir=".") as tempfilepath:
            res = resource(f"sqlite:///{tempfilepath}?sql={sql}")
            data = res.read_json()
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

    def test_write(self):
        with NamedClosedTemporaryFile() as tempfilepath:
            res_src = resource(__file__)
            res_tgt = resource(tempfilepath)
            self.assertRaises(
                FileExistsError, partial(res_tgt.write_bytes, b"", overwrite=False)
            )
            res_tgt.write_resource(res_src, overwrite=True)
            self.assertEqual(
                os.path.getsize(__file__),
                os.path.getsize(tempfilepath),
            )

        with NamedClosedTemporaryFile(suffix=".sqlite3", dir=".") as tempfilepath:
            data_in = [{"s": "s1", "i": 1}, {"s": None, "i": 2}]
            res = resource(f"sqlite:///{tempfilepath}?table=test")
            res.write_json(data_in, overwrite=False)
            self.assertRaises(
                Exception, partial(res.write_json, data_in, overwrite=False)
            )
            data_out = res.read_json()
            self.assertEqual(dumps(data_in), dumps(data_out))
