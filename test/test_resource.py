from test import TestCase

from datatools.resource import Resource


class TestResource(TestCase):
    def test_uri(self):
        res = Resource("http://host/path with space/?q=1&q=2")
        self.assertEqual(res.uri, "http://host/path%20with%20space/?q=1&q=2")

        res = Resource(r"c:\path\file")
        res = Resource(res.path)
        self.assertEqual(res.uri, "file:///c%3A/path/file")
        self.assertEqual(res.path, "c:/path/file")

        res = Resource(r"path\file")
        res = Resource(res.path)
        self.assertEqual(res.uri, "file:path/file")
        self.assertEqual(res.path, "path/file")

        res = Resource(r"\\network\$share\file")
        res = Resource(res.path)
        self.assertEqual(res.uri, "file://network/%24share/file")
        self.assertEqual(res.path, "//network/$share/file")

        res = Resource(r"/var/lib/file#fragment?q=1")
        self.assertEqual(res.uri, "file:///var/lib/file#fragment?q=1")
        res = Resource(res.path)
        self.assertEqual(res.path, "/var/lib/file")
