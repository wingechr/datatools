from test import TestCase

from datatools.resource import Resource


class TestResource(TestCase):
    def test_uri(self):
        res = Resource("http://host/path with space/?q=1&q=2")

        for path in [
            r"\\networkdrive\path with space\file.suffix",
            r"\\localhost\C$\path with space\file.suffix",
            r"C:\path with space\file.suffix" r"/path with space/file.suffix",
        ]:
            res = Resource(path)
            print(res.uri)
            self.assertEqual(path.replace("\\", "/"), res.path)
