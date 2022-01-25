from __future__ import annotations

from collections import namedtuple

import asyncio
import pickle
import sys
import typing as t
from difflib import SequenceMatcher
from functools import partial, wraps, reduce
from warnings import warn

import ubii.proto as ub
from . import RegistryMeta
from .typing import S, T, ExcInfo


def similar(choices, item, cutoff=0.70):
    _similarity = lambda k: SequenceMatcher(None, k, item).ratio()  # noqa
    return list(sorted(filter(lambda item: _similarity(item) > cutoff, choices), key=_similarity, reverse=True))


T_Callable = t.TypeVar('T_Callable', bound=t.Callable[..., t.Any])


class calc_delta:
    def __init__(self, get_value):
        self.get_value = get_value
        self.value = get_value()

    def __call__(self):
        previous = self.value
        self.value = self.get_value()
        return self.value - previous


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


class registry(t.Generic[S, T]):
    def __init__(self: registry[S, T], instance_key: t.Callable[[T], S], fn: t.Callable[..., T]):
        self.key = instance_key
        self.func = fn
        self._registry: t.Dict[S, T] = {}
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


def exc_handler_decorator(handler: t.Callable[[ExcInfo], t.Awaitable[None | bool] | None | bool]):
    def decorator(fun):
        @wraps(fun)
        async def _inner(*args, **kwargs):
            try:
                result = fun(*args, **kwargs)
                if asyncio.isfuture(result) or asyncio.iscoroutine(result):
                    result = await result
                return result
            except:  # noqa
                exception_info = sys.exc_info()
                result = handler(*exception_info)
                if asyncio.isfuture(result) or asyncio.iscoroutine(result):
                    await result

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


class AbstractAnnotations:
    attr_name = '__required_annotations__'
    wrap_marker = object()

    def __init__(self, *names):
        self.names = names

    def __call__(self, cls: type):
        if any(name not in cls.__annotations__ for name in self.names):
            raise ValueError(f"{cls} needs to have annotation[s] for {', '.join(self.names)}")

        names = getattr(cls, self.attr_name, set())
        names.update(self.names)
        setattr(cls, self.attr_name, names)

        original_new = cls.__new__

        wrapped = self.wrap_once(original_new)  # might return original_new if already decorated
        setattr(cls, original_new.__name__, wrapped)

        return cls

    def wrap_once(self, original_new):
        wrap_markers = getattr(original_new, 'markers', set())
        if self.wrap_marker in wrap_markers:
            return original_new

        @wraps(original_new)
        def wrapped(cls, *args, **kwargs):
            instance = original_new(cls, *args, **kwargs)

            # is instance can be created type(instance) can't have abstract methods, so
            # instance also has to have all required attributes
            missing = [name for name in getattr(cls, self.attr_name, []) if not hasattr(cls, name)]
            if missing:
                raise TypeError(f"Can't create {cls} instance with missing class attribute[s] {', '.join(map(repr, missing))}")

            return instance

        wrap_markers.add(self.wrap_marker)
        wrapped.markers = wrap_markers
        return wrapped


class ProtoRegistry(ub.ProtoMeta, RegistryMeta):
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
        self.waiting = None

    async def _waiter(self):
        async with self.condition:
            await self.condition.wait_for(self.predicate)

    def __await__(self):
        if self.waiting is None:
            self.waiting = self._waiter()

        return self.waiting.__await__()

    def __bool__(self):
        return self.predicate()


class make_dict(t.Generic[S, T]):
    def __init__(self: make_dict[S, T],
                 key: t.Callable[[t.Any], S],
                 value: t.Callable[[t.Any], T],
                 filter_none=False):
        self._key = key
        self._value = value
        self._filter = filter_none

    def __call__(self, iterable: t.Iterable) -> t.Dict[S, T]:
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
    'hook',
    'registry',
    'exc_handler_decorator',
    'log_call',
    'ProtoRegistry',
    'function_chain',
    'compose',
    'awaitable_predicate',
    'make_dict',
    'async_compose',
    'attach_info',
    'calc_delta',
)
