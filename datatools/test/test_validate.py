import logging  # noqa
import unittest

from click.testing import CliRunner

import datatools.__main__


class _TestCase(unittest.TestCase):
    def _test_cli(self, fun_name, args):
        runner = CliRunner()
        fun = getattr(datatools.__main__, fun_name)
        res = runner.invoke(fun, args, catch_exceptions=False)

        if not res.exit_code == 0:
            raise Exception(res.output)

    def _test_cli2(self):
        self._test_cli("validate", ["fp"])

    def test_cli(self):
        self._test_cli("main", [])
