import asyncio
import typing as t
from dataclasses import dataclass
from functools import wraps, cached_property


def make_async(func):
    """
    Decorator to turn a non async function into a coroutine by running it in the default executor pool.
    """

    @wraps(func)
    async def _callback(*args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    return _callback


T = t.TypeVar('T')


@dataclass
class accessor(t.Generic[T]):
    get: t.Callable[[], t.Coroutine[t.Any, t.Any, T]]
    set: t.Callable[[T], t.Coroutine[t.Any, t.Any, None]]


class condition_property(cached_property, t.Generic[T]):
    """
    This is a decorator to create a cached ``accessor`` to handle access to some data via a asyncio.Condition

    You can use it like the normal @property decorator, but the result of the lookup (__get__ of the descriptor) will
    be an ``accessor`` with coroutine attributes to handle safely setting and getting the value (from the objects
    methods passed via ``setter`` and ``getter`` , like in normal properties) by means of a condition.

    The ``get`` coroutine of the accessor always blocks until the ``set`` coroutine of the same accessor sets the value.
    """

    def __init__(self: 'condition_property[T]',
                 fget: t.Optional[t.Callable[[t.Any], T]] = ...,
                 fset: t.Optional[t.Callable[[t.Any, T], None]] = ...,
                 fdel: t.Optional[t.Callable[[t.Any], None]] = ...,
                 doc: t.Optional[str] = ...) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc
        super().__init__(self._create_accessor)

    @t.overload
    def __get__(self: 'condition_property[T]', instance: None,
                owner: t.Optional[t.Type[t.Any]] = ...) -> 'condition_property[T]':
        ...

    @t.overload
    def __get__(self: 'condition_property[T]', instance: object, owner: t.Optional[t.Type[t.Any]] = ...) -> accessor[T]:
        ...

    def __get__(self, instance: None, owner: t.Optional[t.Type[t.Any]] = ...) -> 'condition_property[T]':
        return super().__get__(instance, owner)

    def __set__(self, obj, value):
        raise AttributeError(f"can't set attribute directly, use set()")

    def _create_accessor(self, obj):
        condition = asyncio.Condition()
        return accessor(
            get=lambda: self._get(condition=condition, obj=obj),
            set=lambda value: self._set(condition=condition, obj=obj, value=value)
        )

    async def _set(self, *, condition: asyncio.Condition, obj: t.Any, value):
        if self.fset is None:
            raise AttributeError(f"can't set attribute {self.attrname}")
        async with condition:
            self.fset(obj, value)
            condition.notify_all()

    async def _get(self, *, condition: asyncio.Condition, obj: t.Any):
        if self.fget is None:
            raise AttributeError(f'unreadable attribute {self.attrname}')
        async with condition:
            await condition.wait()
            return self.fget(obj)

    def getter(self, fget):
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def setter(self, fset):
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def deleter(self, fdel):
        prop = type(self)(self.fget, self.fset, fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop


class CoroutineWrapper(t.Coroutine):
    """
    Complex Coroutines are easy to implement with native ``def async`` coroutine syntax, but often require
    some smaller coroutines to compose. Inheriting from CoroutineWrapper, a complex coroutine can encapsulate
    all it's dependencies and auxiliary methods.
    """

    def __init__(self, *, coroutine):
        self._coroutine = coroutine

    def __await__(self):
        return self._coroutine.__await__()

    def send(self, value):
        return self._coroutine.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coroutine.throw(typ)
        elif tb is None:
            return self._coroutine.throw(typ, val)
        else:
            return self._coroutine.throw(typ, val, tb)

    def close(self):
        return self._coroutine.close()