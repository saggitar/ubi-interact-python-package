from __future__ import annotations

from collections import namedtuple

import asyncio
import pickle
import sys
import typing as t
from difflib import SequenceMatcher
from functools import partial, wraps, reduce
from itertools import chain
from warnings import warn

import codestare.async_utils as utils
import ubii.proto as ub
from .typing import _S, _T, _ExcInfo, _T_EnumFlag


def similar(choices, item, cutoff=0.70):
    _similarity = lambda k: SequenceMatcher(None, k, item).ratio()  # noqa
    return list(sorted(filter(lambda item: _similarity(item) > cutoff, choices), key=_similarity, reverse=True))


class EnumMatcher:
    _enum_tuple = t.Tuple[_T_EnumFlag, ...]
    _no_default = object()

    @classmethod
    def matches(cls, base: _enum_tuple, query: _enum_tuple) -> bool:
        if not len(base) == len(query):
            return False

        if base == query:
            return True

        if any(x is None for x in chain(base, query)):
            return False

        return all(b & q == q for b, q in zip(base, query))

    @classmethod
    def get_matching_value(cls, key: _enum_tuple, default: t.Any = _no_default, *,
                           mapping: t.Mapping[_enum_tuple, _T],
                           ) -> _T:
        matching = [value for enums, value in mapping.items() if cls.matches(enums, key)]
        if len(matching) != 1 and default is EnumMatcher._no_default:
            raise KeyError(f"Found matching values {matching} for query {key}, not exactly one match")

        return matching[0] if matching else default


T_Callable = t.TypeVar('T_Callable', bound=t.Callable[..., t.Any])


class hook(t.Generic[T_Callable]):
    __name__: str

    def __init__(self: hook[T_Callable], func: T_Callable, decorators=None):
        self.func = func
        self._decorators = decorators or set()
        self._applied = None
        wraps(func)(self)

    @property
    def decorators(self):
        return self._decorators

    def register_decorator(self, decorator):
        self._decorators.add(decorator)
        self.cache_clear()

    def cache_clear(self):
        """
        Apply decorators again at next access
        """
        self._applied = None

    def __call__(self, *args, **kwargs):
        if self._applied is None:
            self._applied = compose(*self.decorators)(self.func)

        return self._applied(*args, **kwargs)

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return partial(self, instance)


class registry(t.Generic[_S, _T]):
    def __init__(self: registry[_S, _T], instance_key: t.Callable[[_T], _S], fn: t.Callable[..., _T]):
        self.key = instance_key
        self.func = fn
        self._registry: t.Dict[_S, _T] = {}
        wraps(fn)(self)

    @property
    def registry(self):
        return self._registry

    def __call__(self, *args, **kwargs):
        instance = self.func(*args, **kwargs)
        self._registry[self.key(instance)] = instance
        return instance

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return partial(self, instance)


def exc_handler(handler: t.Callable[[_ExcInfo], None]):
    def decorator(fun):
        @wraps(fun)
        async def _inner(*args, **kwargs):
            try:
                result = fun(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
                return result
            except Exception as e:
                exception_info = sys.exc_info()
                handler(*exception_info)
                raise e

        return _inner

    return decorator


def log_call(logger):
    def decorator(fun):
        @wraps(fun)
        def __inner(*args):
            logger.debug(f"called {fun}")
            return fun(*args)

        return __inner

    return decorator


class ProtoRegistry(ub.ProtoMeta, utils.RegistryMeta):
    """

    """

    def __new__(mcs, *args, **kwargs):
        kls = super().__new__(mcs, *args, **kwargs)
        return kls

    def _serialize_all(cls):
        return {key: cls.serialize(obj) for key, obj in cls.registry.items()}

    def _deserialize_all(cls, mapping: t.Mapping):
        return {key: cls.deserialize(obj) for key, obj in mapping.items()}

    def save_specs(cls, path):
        """
        Serialize all registered Protocol Buffer Wrapper objects
        :param path:
        :type path:
        :return:
        :rtype:
        """
        with open(path, 'wb') as file:
            pickle.dump(cls._serialize_all(), file)

    def update_specs(cls, path):
        with open(path, 'rb') as file:
            loaded = pickle.load(file)

        specs = cls._deserialize_all(loaded)
        for key, item in cls.registry.items():
            spec = specs.get(key)
            if not spec:
                warn(f"No {cls} instance for key {key} registered, can't update")
                continue

            cls.copy_from(item, spec)


class function_chain:
    def __init__(self, *funcs):
        self.funcs = funcs

    def __call__(self, *args):
        for f in self.funcs:
            f(*args)

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return partial(self, instance)

    @classmethod
    def reverse(cls, *funcs):
        return cls(*reversed(funcs))


class compose:
    def __init__(self, *fns):
        self._info = ', '.join(map(repr, fns))
        self.reduced = reduce(lambda g, f: lambda *a: f(g(*a)), fns) if fns else (lambda x: x)

    def __call__(self, *args):
        return self.reduced(*args)

    def __repr__(self):
        return f"compose({self._info})"


class awaitable_predicate:
    def __init__(self, predicate: t.Callable[[], bool], condition: asyncio.Condition | None = None):
        self.condition = condition or asyncio.Condition()
        self.predicate = predicate
        self._waiter: t.Awaitable | None = None

    async def __waiter(self):
        async with self.condition:
            await self.condition.wait_for(self.predicate)

    def __bool__(self):
        return self.predicate()

    def __await__(self):
        if self._waiter is None:
            self._waiter = self.__waiter()

        return self._waiter.__await__()


class make_dict(t.Generic[_S, _T]):
    def __init__(self: make_dict[_S, _T],
                 key: t.Callable[[t.Any], _S],
                 value: t.Callable[[t.Any], _T],
                 filter_none=False):
        self._key = key
        self._value = value
        self._filter = filter_none

    def __call__(self, iterable: t.Iterable) -> t.Dict[_S, _T]:
        return {
            self._key(item): self._value(item)
            for item in iterable
            if not self._filter or self._value(item)
        }


class async_compose:
    def __init__(self, *fns):
        def __compose(g: t.Callable[[t.Any], t.Coroutine], f: t.Callable[[t.Any], t.Coroutine]):
            async def composed(*args):
                return await(f(await g(*args)))

            return composed

        self._reduced = reduce(__compose, fns)

    def __call__(self, *args):
        return self._reduced(*args)


class attach_info:
    result = namedtuple('result', ['value', 'info'])

    def __init__(self, info, func: t.Callable):
        self._reduced: t.Union[async_compose, compose]
        if asyncio.iscoroutinefunction(func):
            async def attach(result):
                return self.result(value=result, info=info)

            self._reduced = async_compose(func, attach)
        else:
            def attach(result):
                return self.result(value=result, info=info)

            self._reduced = compose(func, attach)

    def __call__(self, *args):
        return self._reduced(*args)


__all__ = (
    'similar',
    'EnumMatcher',
    'hook',
    'registry',
    'exc_handler',
    'log_call',
    'ProtoRegistry',
    'function_chain',
    'compose',
    'awaitable_predicate',
    'make_dict',
    'async_compose',
    'attach_info'
)
