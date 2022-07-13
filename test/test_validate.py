from functools import partial
from test import TestCase

from jsonschema.exceptions import SchemaError

from datatools.utils.json import SchemaValidator, validate_resource


class TestJsonSchema(TestCase):
    def test_invalid_schema(self):
        self.assertRaises(
            SchemaError,
            partial(SchemaValidator, {"type": "invalid"}),
        )

    def test_type(self):
        """
        The value of this keyword MUST be either a string or an array.
        If it is an array, elements of the array MUST be strings and MUST be unique.

        Options: "null", "boolean", "object", "array", "number", "string", "integer"
        """

        validator = SchemaValidator({"type": ["string", "boolean"]})
        validator.validate("i am string")
        validator.validate(True)
        self.assertRaises(Exception, partial(validator.validate, 1))

        validator = SchemaValidator({"type": "integer"})
        self.assertRaises(Exception, partial(validator.validate, "100"))
        validator.validate(100)

    def test_enum(self):
        """
        The value of this keyword MUST be an array. This array SHOULD have at
        least one element.
        Elements in the array SHOULD be unique.
        """

        validator = SchemaValidator({"enum": ["v1", "v2"]})
        validator.validate("v1")
        self.assertRaises(Exception, partial(validator.validate, "V2"))

    def test_maximum_minimum(self):
        """
        The value of "maximum" MUST be a number, representing an inclusive upper
        limit for a numeric instance.
        The value of "exclusiveMaximum" MUST be number, representing an
        exclusive upper limit for a numeric instance.

        Note: this ONLY validates numbers, other values are ok
        """

        validator = SchemaValidator({"maximum": 100})
        validator.validate(-1)
        validator.validate(100)
        self.assertRaises(Exception, partial(validator.validate, 100.01))

        validator = SchemaValidator({"exclusiveMaximum": 100})
        validator.validate(-1)
        self.assertRaises(Exception, partial(validator.validate, 100))

        validator = SchemaValidator({"exclusiveMinimum": float("-inf")})
        validator.validate(0)
        self.assertRaises(Exception, partial(validator.validate, float("-inf")))

        # non numeric are ok
        validator = SchemaValidator({"maximum": 100})
        validator.validate("1000000")

    def test_min_max_length(self):
        """
        The value of this keyword MUST be a non-negative integer.
        A string instance is valid against this keyword if its length is less
        than, or equal to, the value of this keyword.

        Note: this ONLY validates strings, other values are ok
        """

        validator = SchemaValidator({"minLength": 5, "maxLength": 5})
        validator.validate("09112")
        self.assertRaises(Exception, partial(validator.validate, "9112"))

        # non string are ok
        validator.validate(9112)

    def test_min_max_items(self):
        """
        The value of this keyword MUST be a non-negative integer.
        An array instance is valid against "maxItems" if its size is less than,
        or equal to, the value of this keyword.
        """
        validator = SchemaValidator({"minItems": 1, "maxItems": 2})
        validator.validate(None)  # only tests arrays
        validator.validate([1])
        validator.validate([1, None])
        self.assertRaises(Exception, partial(validator.validate, []))
        self.assertRaises(Exception, partial(validator.validate, [1, 2, 3]))

    def test_required(self):
        """
        The value of this keyword MUST be an array.
        Elements of this array, if any, MUST be strings, and MUST be unique.

        An object instance is valid against this keyword if every item in the
        array is the name of a property in the instance.
        """

        validator = SchemaValidator({"required": ["a", "b"]})
        validator.validate({"a": 1, "b": 2, "c": 3})
        self.assertRaises(Exception, partial(validator.validate, {"a": 1, "c": 3}))

    def test_pattern(self):
        """
        The value of this keyword MUST be a string.
        This string SHOULD be a valid regular expression,
        according to the ECMA 262 regular expression dialect.
        """

        validator = SchemaValidator({"pattern": "^a.*$"})
        validator.validate("aa")
        self.assertRaises(
            Exception, partial(validator.validate, "Aa")
        )  # case sensitive

        validator = SchemaValidator({"pattern": "a[0-9]{1,2}"})
        validator.validate("a1x")
        validator.validate("a11x")
        self.assertRaises(Exception, partial(validator.validate, "ax"))


class TestFrictionless(TestCase):
    """Frictionless validators return a list of errors"""

    @staticmethod
    def create_single_val_resource(value, **field_specs):
        scm = {
            "name": "res1",
            "schema": {"fields": [{"name": "fld1"}]},
            "profile": "tabular-data-resource",
            "data": [],
            "dialect": {"header": False},  # no header row in data
        }
        scm["schema"]["fields"][0].update(field_specs)
        scm["data"].append([value])
        return scm

    def test_resource(self):
        self.assertRaises(
            Exception, partial(validate_resource, {"name": "name"})
        )  # must have data (or path)
        self.assertRaises(
            Exception, partial(validate_resource, {"name": "name", "data": [[]]})
        )  # empty data is also an error
        validate_resource({"name": "name", "data": [[99]]})

    def test_type(self):
        """
        string
        number
        integer
        boolean

        date
        time
        datetime

        also: any, object, array

        IMPORTANT:
            * canonical strings that can be cast as target type are valid.
            * key constraints are not validated (but unique/required is)

        """

        # numbers
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("text", type="integer"),
            ),
        )
        self.assertRaises(
            Exception,
            partial(
                validate_resource, self.create_single_val_resource(99.9, type="integer")
            ),
        )
        validate_resource(self.create_single_val_resource("99", type="integer"))
        validate_resource(self.create_single_val_resource(99, type="integer"))
        validate_resource(self.create_single_val_resource("99.9", type="number"))
        validate_resource(self.create_single_val_resource(99.9, type="number"))

        # strings
        self.assertRaises(
            Exception,
            partial(
                validate_resource, self.create_single_val_resource(99, type="string")
            ),
        )
        validate_resource(self.create_single_val_resource("99", type="string"))

        # string + format
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("abc", type="string", format="uri"),
            ),
        )
        validate_resource(
            self.create_single_val_resource(
                "http://abc.de", type="string", format="uri"
            )
        )

        # boolean
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("aaaa", type="boolean"),
            ),
        )
        self.assertRaises(
            Exception,
            partial(
                validate_resource, self.create_single_val_resource(0, type="boolean")
            ),
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
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("01.01.1970", type="date"),
            ),
        )
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("1970-01", type="date"),
            ),
        )
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("1970-02-31", type="date"),
            ),
        )
        self.assertRaises(
            Exception,
            partial(validate_resource, self.create_single_val_resource(0, type="date")),
        )
        validate_resource(self.create_single_val_resource("1970-01-01", type="date"))

        self.assertRaises(
            Exception,
            partial(
                validate_resource, self.create_single_val_resource("10:11", type="time")
            ),
        )
        validate_resource(self.create_single_val_resource("10:11:12", type="time"))
        validate_resource(
            self.create_single_val_resource("10:11:12.13", type="time")
        )  # milliseconds ok too

        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource("1970-01", type="datetime"),
            ),
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
            partial(
                validate_resource,
                self.create_single_val_resource(True, type=["boolean", "null"]),
            ),
        )

    def test_constraints(self):
        # missing/required
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource(
                    None, type="string", constraints={"required": True}
                ),
            ),
        )
        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource(
                    "", type="string", constraints={"required": True}
                ),
            ),
        )  # this should actually be valid, but its not

        self.assertRaises(
            Exception,
            partial(
                validate_resource,
                self.create_single_val_resource(
                    "N/A",
                    type="string",
                    constraints={"required": True, "missingValues": ["N/A"]},
                ),
            ),
        )

        # minLength/maxLength
        res = self.create_single_val_resource(
            "09111", type="string", constraints={"minLength": 5, "maxLength": 5}
        )
        validate_resource(res)
        res["data"][0][0] = "1234"
        self.assertRaises(Exception, partial(validate_resource, res))

        # minimum / maximum
        res = self.create_single_val_resource(
            "1980-01-01",
            type="date",
            constraints={"minimum": "1970-01-01", "maximum": "2020-01-01"},
        )
        validate_resource(res)
        res["data"][0][0] = "1900-01-01"
        self.assertRaises(Exception, partial(validate_resource, res))

        # pattern
        res = self.create_single_val_resource(
            "a1x", type="string", constraints={"pattern": "^a[0-9]{1,2}.*$"}
        )
        validate_resource(res)
        res["data"][0][0] = "ax"
        self.assertRaises(Exception, partial(validate_resource, res))

        # enum
        res = self.create_single_val_resource(
            "a", type="string", constraints={"enum": ["a", "b"]}
        )
        validate_resource(res)
        res["data"][0][0] = "c"
        self.assertRaises(Exception, partial(validate_resource, res))

        # unique
        res = self.create_single_val_resource(
            10, type="integer", constraints={"unique": True}
        )
        validate_resource(res)
        res["data"].append([11])
        validate_resource(res)
        res["data"].append([10])
        self.assertRaises(Exception, partial(validate_resource, res))
