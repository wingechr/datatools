import os
from functools import partial
from tempfile import TemporaryDirectory

from datatools.location import MemoryLocation, location
from datatools.test import TestCase
from datatools.utils.json import dumpb, guess_data_schema
from datatools.utils.temp import NamedClosedTemporaryFile


class TestResource(TestCase):
    def test_read(self):
        res = location(__file__)
        bytes = res.read()
        self.assertEqual(len(bytes), os.path.getsize(__file__))

        res = location("http://example.com")
        text = res.read()
        self.assertTrue(len(text) > 0)

        sql = "select 1 as one, null as na"

        # in memory
        res = location(f"sqlite://?sql={sql}")
        data = res.read(as_json=True)
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

        # in file (absolute path)
        with NamedClosedTemporaryFile(suffix=".sqlite3") as tempfilepath:
            res = location(f"sqlite:///{tempfilepath}?sql={sql}")
            # also check hash validation
            data = res.read(
                as_json=True,
                bytes_hash="sha256:95a6249e7b6320a180257f4834fd274154f4272a38c51d2e6675ccdbabe42852",  # noqa
            )
        self.assertEqual(data[0]["one"], 1)
        self.assertEqual(data[0]["na"], None)

    def test_write(self):
        with NamedClosedTemporaryFile() as tempfilepath:
            res_src = location(__file__)
            res_tgt = location(tempfilepath)
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
            res = location(f"sqlite:///{tempfilepath}?table=test")
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
            res = location(tempdir + "#test.json")
            report = res.write(data_in)
            self.assertTrue(os.path.isfile(tempdir + "/datapackage.json"))
            self.assertEqual(
                report["hash"],
                "sha256:deaa5af2ea765ed64dc21cbd06baf69462c5d5ae7818fac0966a52e77bea7aff",  # noqa
            )

            # unchecked overwrite fails
            self.assertRaises(Exception, res.write, data_in)
            res.write(data_in, overwrite=True)  # works now

            report = location(tempdir + "#test2.json").write(data_in)
            self.assertTrue(os.path.isfile(tempdir + "/data/test2.json"))
            # same hash, but different name (only data is hashed)
            self.assertEqual(
                report["hash"],
                "sha256:deaa5af2ea765ed64dc21cbd06baf69462c5d5ae7818fac0966a52e77bea7aff",  # noqa
            )

            # check if global hash is updated
            hash = location(tempdir + "/datapackage.json").read(as_json=True)["hash"]
            self.assertEqual(
                hash,
                "sha256:0730b410d31df039f3c284ea0ac28cd27eb417bf13d078c98e854a8cf008c519",  # noqa
            )

            # load dp
            data = location(tempdir + "#test.json").read()
            self.assertEqual(data, dumpb(data_in))

    def test_validate_json_schema(self):
        # use in memory resource
        res = MemoryLocation({"key": 9})

        # this should fail (wrong type)
        self.assertRaises(
            Exception,
            partial(res.read, as_json=True, validate_json_schema={"type": "array"}),
        )

        # this should fail (schema broken)
        self.assertRaises(
            Exception, partial(res.read, as_json=True, validate_json_schema={"type": 1})
        )

        # this should work
        res.read(as_json=True, json_schema={"type": "object"})

    def test_validate_data_schema(self):
        data = [{"i": 1, "s": "s1"}, {"s": None, "i": 2}]
        schema = {
            "fields": [
                {"type": "integer", "name": "i"},
                {"type": "string", "name": "s"},
            ]
        }

        res = MemoryLocation(data)

        # fail: no schema
        self.assertRaises(
            Exception, partial(res.read, as_json=True, validate_data_schema={})
        )

        # fail: broken schema
        self.assertRaises(
            Exception,
            partial(res.read, as_json=True, validate_data_schema={"fields": None}),
        )

        # fail: invalid schema
        self.assertRaises(
            Exception,
            partial(
                res.read,
                as_json=True,
                validate_data_schema={
                    "fields": [
                        {"type": "integer", "name": "i"},
                        {"type": "integer", "name": "s"},
                    ]
                },
            ),
        )

        # works
        res.read(as_json=True, table_schema=schema)

    def test_guess_dataschema(self):
        data = [{"i": 1, "s": "s1"}, {"s": None, "i": 2}]
        schema = {
            "fields": [
                {"type": "integer", "name": "i"},
                {"type": "string", "name": "s"},
            ]
        }
        guessed_schema = guess_data_schema(data)
        self.assertEqual(dumpb(schema), dumpb(guessed_schema))
