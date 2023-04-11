"""
fix examples from frictionless schemas
"""

import json
import re


def fix_example(key, val):
    assert isinstance(val, str)
    val = re.sub(r"\s+", " ", val).strip()
    # fix missing comma
    val = val.replace('" "', '", "')
    # fix extra comma
    val = val.replace(", ]", "]")
    # fix missing } at end
    if val.startswith("{") and not val.endswith("}"):
        val += "}"
    # fix null sequence
    val = val.replace("\\N", "\\\\N")

    val = json.loads(val)
    if isinstance(val, dict):
        if set(val) == set([key]):
            val = val[key]

    return val


def fix_examples_rec(obj, parent_key=None):
    if isinstance(obj, dict):
        for k, vals in obj.items():
            if k == "examples":
                assert isinstance(vals, list)
                obj[k] = [fix_example(parent_key, v) for v in vals]
            else:
                fix_examples_rec(vals, parent_key=k)
    elif isinstance(obj, list):
        for v in obj:
            fix_examples_rec(v, parent_key)
    else:
        pass


def fix_examples_file(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        data = json.load(file)

    fix_examples_rec(data)

    with open(filepath, "w", encoding="utf-8") as file:
        data = json.dump(data, file, indent=4)


if __name__ == "__main__":
    fix_examples_file("tabular-data-resource.schema.json")
    fix_examples_file("tabular-data-package.schema.json")
