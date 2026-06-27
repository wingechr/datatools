import os
import sys
import json
from datatools.storage.classes import FileDataStorage

data_storage = FileDataStorage(".")

def generate() -> bytes:
    return b"[1, 2]"

def convert(data: list) -> list:
    return [x + 1 for x in data]

def dump(x):
    return json.dumps(x).encode()


job_generate = data_storage.job(generate, {"output": None})
job_convert = data_storage.job(convert, {"output": dump}, {"data": json.loads})

rule convert:
    input:
        "generatad.json"
    output:
        "converted.json"
    run:
        job_convert(output[0], input[0])

rule generate:
    output:
        "generatad.json"
    run:
        job_generate(output[0])
