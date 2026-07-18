"""TODO"""

from io import BytesIO
from unittest import TestCase

import boto3
from moto import mock_aws

from datatools.process.importer import S3Importer, infer_importer_class
from datatools.process.task import Task


class TestImporter(TestCase):
    """TODO"""

    def test_importer_unkown_uri(self):
        """should fail on unknown URI"""
        self.assertRaises(
            NotImplementedError, infer_importer_class, "xyz://bad/protocol"
        )

    def test_invalid_arguments(self):
        """TODO"""

        def test_fun(a, b=1):
            return a + b

        def dump_null(data, name):
            pass

        def read_1(name):
            return 1

        # works
        Task(
            test_fun,
            output_writers={"output": dump_null},
            input_readers={"b": read_1},
        )
        # should not work (output == input)
        self.assertRaises(
            Exception,
            Task,
            test_fun,
            output_writers={"a": dump_null},
        )
        # should not work (invalid input)
        self.assertRaises(
            Exception,
            Task,
            test_fun,
            output_writers={"output": dump_null},
            input_readers={"X": read_1},
        )

    @mock_aws
    def test_import_s3(self):
        """TODO"""
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="test")
        s3.put_object(Bucket="test", Key="test.txt", Body=b"test")

        uri = "s3://test/test.txt"
        imp = S3Importer
        self.assertEqual(imp.get_output_name(uri), "test.txt")

        buf = BytesIO()
        imp.output_write_byte_data(imp.get_data(uri), buf)  # type:ignore
        buf.seek(0)
        self.assertEqual(buf.read(), b"test")
