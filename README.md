# Datatools

## Install

```bash
pip install wingechr-datatools
```

## Example usage

```python
from datatools import StorageTemp as Storage

st = Storage()


def make_data():
    return {"a": 1}


res = st.resource(make_data)

data = res.load()
print(data)

```

## Command line

```bash
Usage: datatools [OPTIONS] COMMAND [ARGS]...

  Script entry point.

Options:
  --version                       Show the version and exit.
  -l, --loglevel [debug|info|warning|error]
                                  [default: info]
  -d, --location TEXT
  -g, --global-location
  --help                          Show this message and exit.

Commands:
  help-all
  res
  search


## search
Usage: datatools search [OPTIONS] [PATTERNS]...

Options:
  --help  Show this message and exit.


## res
Usage: datatools res [OPTIONS] PATH COMMAND [ARGS]...

Options:
  -n, --name TEXT
  --help           Show this message and exit.

Commands:
  meta
  save


## res save
Usage: datatools res PATH save [OPTIONS]

Options:
  --help  Show this message and exit.


## res meta
Usage: datatools res PATH meta [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  query
  update  Multiple key=value pairs


## res meta query
Usage: datatools res PATH meta query [OPTIONS] [KEY]

Options:
  --help  Show this message and exit.


## res meta update
Usage: datatools res PATH meta update [OPTIONS] [METADATA_KEY_VALS]...

  Multiple key=value pairs

Options:
  --help  Show this message and exit.


## help-all
Usage: datatools help-all [OPTIONS]

Options:
  --help  Show this message and exit.

```
