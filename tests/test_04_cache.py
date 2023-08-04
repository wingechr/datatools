import logging

from .test_02_storage import TestBase


class Test_04_Cache(TestBase):
    def __DISABLED__test_cache_decorator(self):
        context = {"counter": 0}

        @self.storage.cache(path_prefix="myproject/cache/")  # use defaults
        def test_fun_sum(a, b):
            logging.debug("running test_fun_sum")
            context["counter"] += 1
            return a + b

        self.assertEqual(context["counter"], 0)
        self.assertEqual(test_fun_sum(1, 1), 2)
        # counted up, because first try
        self.assertEqual(context["counter"], 1)
        self.assertEqual(test_fun_sum(1, 1), 2)
        # not counted up, because cache
        self.assertEqual(context["counter"], 1)
        self.assertEqual(test_fun_sum(1, 2), 3)
        # counted up, because new signature
        self.assertEqual(context["counter"], 2)
