import datetime
from functools import partial
from os.path import getsize
from test import TestCase

from datatools import utils


class TestHash(TestCase):
    def test_hash(self):
        byte_hash = utils.byte.hash
        json_hash = utils.json.hash

        self.assertEqual(
            byte_hash(b"", "sha256"),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        self.assertEqual(
            byte_hash(b"test", "sha256"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        )
        # b'"test"'
        self.assertEqual(
            json_hash("test", "sha256"),
            "4d967a30111bf29f0eba01c448b375c1629b2fed01cdfcc3aed91f1b57d5dd5e",
        )
        # b'null'
        self.assertEqual(
            json_hash(None, "sha256"),
            "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b",
        )

        Iterator = partial(utils.byte.Iterator, hash_method="sha256")

        file_size = getsize(__file__)
        with open(__file__, "rb") as file:
            data = file.read()

        # different methods of reading the file
        it1 = Iterator(__file__, max_size=file_size)  # open from path, limit read
        data1 = it1.read()

        # open from file, and use context
        with Iterator(open(__file__, "rb")) as it2:
            data2 = it2.read()

        # open with bytes, and iterate
        it3 = Iterator(data, chunk_size=100)
        data3 = b""
        for chunk in it3:
            data3 += chunk

        self.assertEqual(data, data1)
        self.assertEqual(data, data2)
        self.assertEqual(data, data3)

        self.assertEqual(file_size, it1.get_current_size())
        self.assertEqual(file_size, it2.get_current_size())
        self.assertEqual(file_size, it3.get_current_size())

        hash1 = it1.get_current_hash()
        self.assertEqual(hash1, it2.get_current_hash())
        self.assertEqual(hash1, it2.get_current_hash())


class TestDatetime(TestCase):
    def test_datetime(self):
        dt_unaware = datetime.datetime.now()
        tz_offset = utils.datetime.get_current_timezone_offset()
        tc_utc = utils.datetime.get_timezone_utc()
        dt_aware_local = utils.datetime.to_timezone(dt_unaware, tz_offset)
        dt_aware_utc = utils.datetime.to_timezone(dt_aware_local, tc_utc)
        self.assertTrue(dt_aware_local.tzinfo)
        self.assertEqual(dt_aware_local, dt_aware_utc)
        now = utils.datetime.now()
        utcnow = utils.datetime.utcnow()
        self.assertTrue((utcnow - now).seconds < 10)


class TestString(TestCase):
    def test_normalize(self):
        normalize = utils.text.normalize
        self.assertEqual(normalize("Hello  World!"), "hello_world")
        self.assertEqual(normalize("helloWorld"), "hello_world")
        self.assertEqual(normalize("_private_4"), "_private_4")
        self.assertEqual(
            normalize("François fährt Straßenbahn zum Café Málaga"),
            "francois_faehrt_strassenbahn_zum_cafe_malaga",
        )


class TestCollections(TestCase):
    def test_maps(self):
        dct = utils.collection.FrozenUniqueMap([(1, 2), (3, 4)])
        self.assertEqual(dct[3], 4)
        self.assertRaises(KeyError, partial(dct.__getitem__, 2))
        self.assertRaises(AttributeError, partial(getattr, dct, "__setitem__"))
        dct = utils.collection.UniqueMap([(1, 2), (3, 4)])
        dct[2] = 1
        self.assertRaises(KeyError, partial(dct.__setitem__, 2, None))


class TestCache(TestCase):
    def test_cache(self):
        cache = utils.cache.FileCache()
        cache[1] = b"test"
        # different data with same id
        self.assertRaises(Exception, partial(cache.__setitem__, 1, b"test2"))
        self.assertEqual(cache[1].read(), b"test")
