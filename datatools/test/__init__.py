import logging
import unittest

from click.testing import CliRunner

import datatools.__main__

SCHEMA_DIR = "datatools/schema"
JSONSCHEMA_JSON = "jsonschema.schema.json"


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)


class TestCase(unittest.TestCase):
    def _test_cli(self, fun_name, args):
        fun = getattr(datatools.__main__, fun_name)
        runner = CliRunner()
        res = runner.invoke(fun, args, catch_exceptions=False)

        if not res.exit_code == 0:
            raise Exception(res.output)
