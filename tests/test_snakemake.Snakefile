import os
import sys
from datatools import FileDataStorage
from datatools.utils import json_dumpb, json_loadb

# example functions
def generate() -> bytes:
    return b"[1, 2]"

def convert(data: list) -> list:
    return [x + 1 for x in data]

# use FileDataStorage to create jobs

data_storage = FileDataStorage(".")
task_generate = data_storage.task(generate, {"output": None}) # "output": None -> already bytes
task_convert = data_storage.task(convert, {"output": json_dumpb}, {"data": json_loadb})

# create rules to links jobs

rule convert:
    input:
        "generatad.json"
    output:
        "converted.json"
    run:
        task_convert(output[0], input[0])

rule generate:
    output:
        "generatad.json"
    run:
        task_generate(output[0])

# for this simple exmaple, we could also manally just run
# data_storage.task(generate, {"output": None}, skip_finished=True)("generatad.json")
# data_storage.task(convert, {"output": dump}, {"data": json.loads}, skip_finished=True)("generatad.json", "converted.json")
