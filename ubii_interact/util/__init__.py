from __future__ import annotations

import functools

import types
from warnings import warn

import asyncio
from abc import ABC
from asyncio import Task

import typing
from functools import wraps

from typing import Callable, Coroutine, TypeVar, Generic, Type, Dict


def once(func: Callable[..., Coroutine]) -> Callable[..., Task]:
    """
    Wraps a function using the async await syntax and returns a future.
    This makes sure that when the function is awaited, it is only running once.

    :param func:
    :return:
    """
    future = None

    @wraps(func)
    def once_wrapper(*args, **kwargs):
        nonlocal future
        if not future:
            future = asyncio.create_task(func(*args, **kwargs))
        return future

    return once_wrapper


def iterator(value):
    """
    Sometimes we need to have an iterator from something that can either be itself an iterator or a simple value
    :param value: iterator or value
    """
    try:
        yield from value
    except TypeError:
        yield value


def diff_dicts(compare, expected, **kwargs):
    import json
    left = json.dumps(compare, indent=2, sort_keys=True)
    right = json.dumps(expected, indent=2, sort_keys=True)

    import difflib
    diff = difflib.unified_diff(left.splitlines(True), right.splitlines(True), **kwargs)
    return list(diff)


T = TypeVar('T')


class TypedGeneric(Generic[T]):
    """
    This generic class produces a normal type alias, but also sets the internal `_signature` attribute
    accordingly i.e. when you use it like TypedGeneric[int] the resulting alias will have `_signature=int`.
    """
    __types: Dict[str, Type[TypedGeneric]] = {}
    _signature = object

    def __class_getitem__(cls, item):
        alias = super(TypedGeneric, cls).__class_getitem__(item)
        if isinstance(item, TypeVar):
            return alias

        def _repr(_type):
            return f"{_type.__module__}.{_type.__qualname__}" if hasattr(_type, '__qualname__') else repr(_type)

        name = _repr(alias)
        kls = types.new_class(name, (alias,), {'signature': item})
        return cls.__types.setdefault(name, typing.cast(Type[TypedGeneric], kls))

    def __init_subclass__(cls, /, signature=object, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._signature = signature

