from .exceptions import ValidationException
from .utils import JsonSerializable, UniqueDict


def validate_name(name):
    if not isinstance(name, str) or name.strip() != name or not name:
        raise ValidationException(name)
    return name


def validate_data(data):
    if not data:
        raise ValidationException("no data")
    return data


def validate_path(path):
    if not path:
        raise ValidationException("no path")
    return path


def validate_resource(resource):
    if isinstance(resource, ResourceBase):
        return resource
    try:
        if "resources" in resource:
            return Package(**resource)
        elif "data" in resource:
            return DataResource(**resource)
        elif "path" in resource:
            return PathResource(**resource)
        else:
            raise NotImplementedError()
    except Exception:
        raise ValidationException("not a resource")


def validate_profile(profile):
    if not isinstance(profile, str) or profile.strip() != profile or not profile:
        raise ValidationException(profile)
    return profile


class ResourceBase(JsonSerializable):
    def __init__(self, name, profile):
        self.name = validate_name(name)
        self.profile = validate_profile(profile)

    def __str__(self):
        return self.name

    @classmethod
    def from_json(cls, data):
        return cls(**data)


class PathResource(ResourceBase):
    def __init__(self, name, path, profile="data-resource"):
        super().__init__(name, profile=profile)
        self.path = validate_path(path)


class DataResource(ResourceBase):
    def __init__(self, name, data, profile="data-resource"):
        super().__init__(name, profile=profile)
        self.data = validate_data(data)


class Package(ResourceBase):
    def __init__(self, name, resources, profile="data-package"):
        super().__init__(name, profile=profile)
        self.resources = []
        self._resources_by_name = UniqueDict()

        for r in resources:
            r = validate_resource(r)
            self.resources.append(r)
            self._resources_by_name[r.name] = r

    def get_resource(self, name):
        return self._resources_by_name[name]
