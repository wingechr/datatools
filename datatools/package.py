from datatools.utils import JsonSerializable, json_dumpb
from datatools.exceptions import ValidationException


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
    profile = "data-resource"

    def __init__(self, name, profile=None):
        self.name = validate_name(name)
        self.profile = validate_profile(profile or self.profile)

    def to_json(self):
        return {"profile": self.profile, "name": self.name}

    def __str__(self):
        return self.name

    @classmethod
    def from_json(cls, data):
        assert data["profile"] == cls.profile
        # remove profile
        data = dict((k, v) for k, v in data.items() if k != "profile")
        return cls(**data)


class PathResource(ResourceBase):
    def __init__(self, name, path, profile=None):
        super().__init__(name, profile=profile)
        self.path = validate_path(path)

    def to_json(self):
        res = super().to_json()
        res["path"] = self.path
        return res


class DataResource(ResourceBase):
    def __init__(self, name, data, profile=None):
        super().__init__(name, profile=profile)
        self.data = validate_data(data)

    def to_json(self):
        res = super().to_json()
        res["data"] = self.data
        return res


class Package(ResourceBase):

    profile = "data-package"

    def __init__(self, name, resources, profile=None):
        super().__init__(name, profile=profile)
        self.resources = []
        self._resources_by_name = {}

        for r in resources:
            r = validate_resource(r)
            if r.name in self._resources_by_name:
                raise ValidationException(r.name)
            self.resources.append(r)
            self._resources_by_name[r.name] = r

    def to_json(self):
        res = super().to_json()
        res["resources"] = [r.to_json() for r in self.resources]
        return res
