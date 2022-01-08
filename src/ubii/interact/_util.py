from __future__ import annotations

from warnings import warn

import pickle
from collections import defaultdict

import asyncio
import sys
import typing as t
from difflib import SequenceMatcher
from functools import partial, wraps
from itertools import chain

import ubii.proto as ub
import codestare.async_utils as utils

from . import _typing


def similar(choices, item, cutoff=0.75):
    _similarity = lambda k: SequenceMatcher(None, k, item).ratio()  # noqa
    return list(sorted(filter(lambda item: _similarity(item) > cutoff, choices), key=_similarity, reverse=True))


class EnumMatcher:
    _enum_tuple = t.Tuple[_typing.T_EnumFlag, ...]
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
                           mapping: t.Mapping[_enum_tuple, _typing.T],
                           ) -> _typing.T:
        matching = [value for enums, value in mapping.items() if cls.matches(enums, key)]
        if len(matching) != 1 and default is EnumMatcher._no_default:
            raise KeyError(f"Found matching values {matching} for query {key}, not exactly one match")

        return matching[0] if matching else default


class apply:
    """
    Proxy decorator to apply multiple decorators
    """
    decorators: t.Set[_typing.Decorator]

    def __init__(self, decorators: t.Set[_typing.Decorator] | None = None):
        self.decorators = set() if decorators is None else decorators

    def __call__(self, func: t.Callable):
        for dec in self.decorators:
            func = dec(func)

        return func


class decorator_property:
    _decorators_for_owner: t.Dict[t.Type[t.Any], t.Set[_typing.Decorator]] = defaultdict(set)

    def __init__(self, func, registry=None, decorators=None):
        self._registry = {} if registry is None else registry
        self.func = func
        self._applied = None
        self.name: str | None = None
        self._decorators = decorators or set()

    @property
    def decorators(self):
        # decorator access might change the decorators
        return self._decorators_for_owner

    def register_decorator(self, owner, decorator):
        self._decorators_for_owner[owner].add(decorator)
        self.cache_clear()

    def cache_clear(self):
        """
        Apply decorators again at next access
        """
        self._applied = None

    def __set_name__(self, owner, name):
        self._registry[(owner, name)] = self
        self._decorators_for_owner[owner].update(self._decorators)
        del self._decorators
        self.name = name

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        if self._applied is None:
            owner = owner or type(instance)
            self._applied = apply(self.decorators[owner])(self.func)

        return partial(self._applied, instance)


def register_for_decorator(func=None, *, registry: t.Mapping | None = None,
                           decorators: t.Set[_typing.Decorator] | None = None):
    if func is None:
        return partial(decorator_property, registry=registry, decorators=decorators)
    else:
        return decorator_property(func=func, registry=registry, decorators=decorators)


def exc_handler(exc_handler: t.Callable[[_typing.ExcInfo], None]):
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
                exc_handler(*exception_info)
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
        return {key: cls.deserialize(obj) for key, obj in mapping}

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
