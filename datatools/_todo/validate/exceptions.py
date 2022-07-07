class ValidationException(Exception):
    pass


class ValidationNotImplementedError(ValidationException):
    pass


class NullableException(ValidationException):
    pass


class ConversionException(ValidationException):
    pass
