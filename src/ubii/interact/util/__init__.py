from __future__ import annotations

import types
import asyncio
from asyncio import Task
import typing as t
from functools import wraps

T = t.TypeVar('T')


def once(func: t.Callable[..., t.Coroutine[t.Any, t.Any, T]]) -> t.Callable[..., Task[T]]:
    """
    Wraps a function using the async await syntax and returns a future.
    This makes sure that when the function is awaited, it is only running once.

    :param func: typically an async defined method / function
    :return: a callable returning the Task running `func`
    """
    future = None

    @wraps(func)
    def wrapper(*args, **kwargs) -> Task[T]:
        nonlocal future
        if not future:
            future = asyncio.create_task(func(*args, **kwargs))
        return future

    return wrapper


def iterator(value):
    """
    Sometimes we need to have an iterator from something that can either be itself an iterator or a simple value
    :param value: iterator or value
    """
    try:
        yield from value
    except TypeError:
        yield value


class TypedGeneric(t.Generic[T]):
    """
    This generic class produces a normal type alias, but also sets the internal `_signature` attribute
    accordingly i.e. when you use it like TypedGeneric[int] the resulting alias will have `_signature=int`.
    """
    __types: t.Dict[str, t.Type[TypedGeneric]] = {}
    _signature = object

    def __class_getitem__(cls, item):
        alias = super(TypedGeneric, cls).__class_getitem__(item)
        if isinstance(item, t.TypeVar):
            return alias

        def _repr(_type):
            return f"{_type.__module__}.{_type.__qualname__}" if hasattr(_type, '__qualname__') else repr(_type)

        name = _repr(alias)
        kls = types.new_class(name, (alias,), {'signature': item})
        return cls.__types.setdefault(name, t.cast(t.Type[TypedGeneric], kls))

    def __init_subclass__(cls, /, signature=object, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._signature = signature

