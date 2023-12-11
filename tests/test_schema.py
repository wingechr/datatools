import unittest

from datatools.schema import ValidationError, validate


class TestSchema(unittest.TestCase):
    def test_invalid_schema(self):
        data_good = [{"a": 1, "b": "x"}]
        data_bad = [{"a": 1, "b": "x"}]

        # auto detect frictionless table schema
        validate(
            data_good,
            {
                "fields": [
                    {"name": "a", "type": "integer"},
                    {"name": "b", "type": "string"},
                ]
            },
        )

        self.assertRaises(
            ValidationError,
            validate,
            data_bad,
            {
                "fields": [
                    {"name": "a", "type": "integer"},
                    {"name": "b", "type": "integer"},
                ]
            },
        )

        # auto detect json schema
        validate(
            data_good,
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
                },
            },
        )

        self.assertRaises(
            ValidationError,
            validate,
            data_bad,
            {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                },
            },
        )
