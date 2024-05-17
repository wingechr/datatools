import logging

from datatools.utils import json_dumps

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


def objects_euqal(left, right):
    left = json_dumps(left, sort_keys=True)
    right = json_dumps(right, sort_keys=True)
    return left == right
