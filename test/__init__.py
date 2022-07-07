import logging
import unittest

from click.testing import CliRunner

import datatools.__main__

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)


class TestCase(unittest.TestCase):
    def _test_cli(self, fun_name, args):
        fun = getattr(datatools.__main__, fun_name)
        runner = CliRunner()
        result = runner.invoke(fun, args)
        # output = result.output
        self.assertEqual(result.exit_code, 0)
