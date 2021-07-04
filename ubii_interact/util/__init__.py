import dataclasses
import inspect
import re

import sys
from typing import Optional, Callable, Union
import builtins

def iterator(value):
    """
    Sometimes we need to have an iterator from something that can either be itself an iterator or a simple value
    :param value: iterator or value
    """
    try:
        yield from value
    except TypeError:
        yield value


class UbiiError(Exception):
    def __init__(self, title=None, message=None, stack=None):
        super().__init__(message)
        self.title = title
        self.message = message
        self.stack = stack
