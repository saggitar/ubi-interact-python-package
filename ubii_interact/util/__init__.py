import argparse
import contextlib
import json
import logging

import proto
import sys
from importlib.resources import read_text as _read_text_resource

# configure logging on import
_parser = argparse.ArgumentParser()
_parser.add_argument('--verbose', '-v', action='count', default=0)
_args = _parser.parse_args()
logging.basicConfig(level=logging.ERROR - 10 * _args.verbose)

if not sys.warnoptions:
    import os, warnings
    warnings.simplefilter("default")  # Change the filter in this process
    os.environ["PYTHONWARNINGS"] = "default"  # Also affect subprocesses


class __jsondict__(object):
    def __init__(self, fromdict=None) -> None:
        super().__init__()
        self.data = fromdict or {}

    def get(self, *items):
        extracted = self
        for item in items:
            extracted = extracted[item]

        if isinstance(extracted, dict):
            extracted = __jsondict__(fromdict=extracted)

        return extracted

    def __getitem__(self, key):
        if not '.' in key:
            return self.data[key]
        else:
            keys = key.split('.')
            return self.get(*keys)

    def __setitem__(self, key, value):
        if not '.' in key:
            self.data[key] = value
        else:
            keys = key.split('.')
            container = self.get(*keys[:-1])
            container[keys[-1]] = value

    def __getattr__(self, item):
        return self.get(item)

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return str(self.data)


constants = json.loads(_read_text_resource(proto, "constants.json"))
constants = __jsondict__(fromdict=constants)

UBII_SERVICE_URL = 'UBII_SERVICE_URL'

def apply(fun, item):
    if isinstance(item, dict):
        return {k: apply(fun, v) for k, v in item.items()}
    else:
        return fun(item)


@contextlib.contextmanager
def tmpfile(location, content=None):
    from pathlib import Path
    location = Path(location)
    with location.open('w') as f:
        if content:
            f.write(content)
        yield f

    location.unlink()