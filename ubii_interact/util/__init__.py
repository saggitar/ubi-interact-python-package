import asyncio

from typing import Any, Dict

import contextlib
import json
import proto
from importlib.resources import read_text as _read_text_resource

class __jsondict__(object):
    def __init__(self, fromdict=None) -> None:
        super().__init__()
        self.data = fromdict or {}

    def __contains__(self, item):
        """
        Since items can't be set to None, this check is ok.
        """
        return self[item] is not None

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
        if value is None:
            return ValueError(f"value is {value}")

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


def makelist(obj):
    try:
        return list(obj)
    except TypeError:
        return [obj]

class PushDict(object):
    def __init__(self, from_dict=None, filter=None):
        self._filter = filter
        self._data = dict(from_dict) if from_dict else {}

    def update(self, kwargs):
        for k, v in kwargs.items():
            v = makelist(v)
            self._data[k] = self._data.setdefault(k, v[:1]) + v[1:]

    def clear(self, key):
        self._data[key].clear()

    def items(self):
        return self._data.items()

    def setdefault(self, key, item):
        return self._data.setdefault(key, item)

    def __getitem__(self, item):
        if item in self._filter:
            item = self._filter[item]

        return self._data[item]

    def __bool__(self):
        return bool(self._data)


def as_iterator(value):
    """
    Sometimes we need to have an iterator from something that can either be itself an iterator or a simple value
    :param value: iterator or value
    """
    try:
        yield from value
    except TypeError:
        yield value


