"""
Have a look at the specs at
* https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.6.1
* https://specs.frictionlessdata.io/table-schema/#field-descriptors
* https://github.com/frictionlessdata/specs

"""

import unittest

from datatools.exceptions import ValidationError
from datatools.schema import validate_resource


class TestFrictionless(unittest.TestCase):
    """Frictionless validators return a list of errors"""

    @staticmethod
    def create_single_val_resource(value, **field_specs):
        scm = {
            "name": "res1",
            "schema": {"fields": [{"name": "field1"}]},
            "profile": "tabular-data-resource",
            "data": [{"field1": value}],
        }
        scm["schema"]["fields"][0].update(field_specs)
        return scm

    def test_resource(self):
        self.assertRaises(
            ValidationError, validate_resource, {"name": "name"}
        )  # must have data (or path)
        self.assertRaises(
            ValidationError, validate_resource, {"name": "name", "data": [[]]}
        )  # empty data is also an error
        validate_resource({"name": "name", "data": [[99]]})

    def test_type(self):
        """
        IMPORTANT:
            * canonical strings that can be cast as target type are valid.
            * key constraints are not validated (but unique/required is)

        """

        # numbers
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("aaa", type="integer"),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(99.9, type="integer"),
        )
        validate_resource(self.create_single_val_resource("99", type="integer"))
        validate_resource(self.create_single_val_resource(99, type="integer"))
        validate_resource(self.create_single_val_resource("99.9", type="number"))
        validate_resource(self.create_single_val_resource(99.9, type="number"))

        # strings
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(99, type="string"),
        )
        validate_resource(self.create_single_val_resource("99", type="string"))

        # string + format
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("abc", type="string", format="uri"),
        )
        validate_resource(
            self.create_single_val_resource(
                "http://abc.de", type="string", format="uri"
            )
        )

        # boolean
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("aaaa", type="boolean"),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(0, type="boolean"),
        )
        validate_resource(self.create_single_val_resource("true", type="boolean"))
        validate_resource(self.create_single_val_resource(True, type="boolean"))

        # boolean + trueValues
        validate_resource(
            self.create_single_val_resource(
                "1",
                type="boolean",
                trueValues=["1", "TRUE"],
                falseValues=["0", "FALSE"],
            )
        )

        # date/times
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("01.01.1970", type="date"),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("1970-01", type="date"),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("1970-02-31", type="date"),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(0, type="date"),
        )
        validate_resource(self.create_single_val_resource("1970-01-01", type="date"))

        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("10:11", type="time"),
        )
        validate_resource(self.create_single_val_resource("10:11:12", type="time"))
        validate_resource(
            self.create_single_val_resource("10:11:12.13", type="time")
        )  # milliseconds ok too

        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource("1970-01", type="datetime"),
        )
        validate_resource(
            self.create_single_val_resource("1970-01-01 10:11:12", type="datetime")
        )
        validate_resource(
            self.create_single_val_resource("1970-01-01T10:11:12", type="datetime")
        )  # T or space is ok
        validate_resource(
            self.create_single_val_resource(
                "1970-01-01 10:11:12+00:10", type="datetime"
            )
        )  # offset is ok too
        validate_resource(
            self.create_single_val_resource("1970-01-01 10:11:12+0010", type="datetime")
        )  # this too
        validate_resource(
            self.create_single_val_resource("1970-01-01 10:11:12Z", type="datetime")
        )  # this too

        # IMPORTANT: does not allow for multiple types
        self.assertRaises(
            Exception,
            validate_resource,
            self.create_single_val_resource(True, type=["boolean", "null"]),
        )

    def test_constraints(self):
        # missing/required
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(
                None, type="string", constraints={"required": True}
            ),
        )
        self.assertRaises(
            ValidationError,
            validate_resource,
            self.create_single_val_resource(
                "", type="string", constraints={"required": True}
            ),
        )

        # minLength/maxLength
        res = self.create_single_val_resource(
            "09111", type="string", constraints={"minLength": 5, "maxLength": 5}
        )
        validate_resource(res)
        res["data"][0][0] = "1234"
        self.assertRaises(ValidationError, validate_resource, res)

        # minimum / maximum
        res = self.create_single_val_resource(
            "1980-01-01",
            type="date",
            constraints={"minimum": "1970-01-01", "maximum": "2020-01-01"},
        )
        validate_resource(res)
        res["data"][0][0] = "1900-01-01"
        self.assertRaises(ValidationError, validate_resource, res)

        # pattern
        res = self.create_single_val_resource(
            "a1x", type="string", constraints={"pattern": "^a[0-9]{1,2}.*$"}
        )
        validate_resource(res)
        res["data"][0][0] = "ax"
        self.assertRaises(ValidationError, validate_resource, res)

        # enum
        res = self.create_single_val_resource(
            "a", type="string", constraints={"enum": ["a", "b"]}
        )
        validate_resource(res)
        res["data"][0][0] = "c"
        self.assertRaises(ValidationError, validate_resource, res)

        # unique
        res = self.create_single_val_resource(
            10, type="integer", constraints={"unique": True}
        )
        validate_resource(res)
        res["data"].append({"field1": 11})
        validate_resource(res)
        res["data"].append({"field1": 10})
        self.assertRaises(ValidationError, validate_resource, res)
