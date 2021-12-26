from __future__ import annotations

import asyncio
import typing as t
from dataclasses import dataclass
from functools import wraps, cached_property

_T = t.TypeVar('_T')
_S = t.TypeVar('_S')
_SimpleCoroutine = t.Coroutine[t.Any, t.Any, _T]


def make_async(func):
    """
    Decorator to turn a non async function into a coroutine by running it in the default executor pool.
    """

    @wraps(func)
    async def _callback(*args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    return _callback


@dataclass(init=True)
class accessor(t.Generic[_S]):
    class getter_t(t.Protocol[_T]):
        @t.overload
        def __call__(self: 'accessor.getter_t[_T]', predicate: t.Callable[[], bool]) -> _SimpleCoroutine[_T]: ...

        @t.overload
        def __call__(self: 'accessor.getter_t[_T]', predicate: None) -> t.Callable[[], _T]: ...

        def __call__(self: 'accessor.getter_t[_T]', predicate: t.Callable[[], bool] | None): ...

    class setter_t(t.Protocol[_T]):
        def __call__(self: 'accessor.setter_t[_T]', value: _T) -> t.Coroutine[t.Any, t.Any, None]: ...

    set: setter_t[_S]
    get: getter_t[_S]


class condition_property(cached_property, t.Generic[_T]):
    """
    This is a decorator to create a cached ``accessor`` to handle access to some data via a asyncio.Condition

    You can use it like the normal @property decorator, but the result of the lookup (__get__ of the descriptor) will
    be an ``accessor`` with coroutine attributes to handle safely setting and getting the value (from the objects
    methods passed via ``setter`` and ``getter`` , like in normal properties) by means of a condition.

    The ``next`` coroutine of the accessor always blocks until the ``set`` coroutine of the same accessor sets the value.
    The ``get`` coroutine of the accessor uses the lock of the condition for exclusive access, but does not block
    until a value is set.
    The ``set`` coroutine of the accessor sets the value and notifies all waiting tasks (every ``next``)
    """
    func: t.Callable[[t.Any], accessor[_T]]

    def __init__(self: condition_property[_T],
                 fget: t.Callable[[t.Any], _T] | None = None,
                 fset: t.Callable[[t.Any, _T], None] | None = None,
                 fdel: t.Callable[[t.Any], None] | None = None,
                 doc: str | None = None) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc
        super().__init__(self._create_accessor)

    def __set__(self, obj, value: _T):
        raise AttributeError(f"can't set attribute directly, use set()")

    def _create_accessor(self: 'condition_property[_T]', obj: object) -> accessor[_T]:
        condition = asyncio.Condition()
        return accessor(
            get=lambda predicate=(lambda: True): (
                self._get(condition=condition, obj=obj, predicate=predicate)
                if predicate else
                self.fget(obj)
            ),
            set=lambda value: self._set(condition=condition, obj=obj, value=value),
        )

    async def _set(self, *,
                   condition: asyncio.Condition,
                   obj: object, value: _T):
        if self.fset is None:
            raise AttributeError(f"can't set attribute {self.attrname}")
        async with condition:
            should_notify = self.fset(obj, value)
            if should_notify is None or should_notify:
                condition.notify_all()

    async def _get(self, *,
                   condition: asyncio.Condition,
                   obj: object,
                   predicate: t.Callable[[], bool] = (lambda: True)):
        if self.fget is None:
            raise AttributeError(f'unreadable attribute {self.attrname}')

        if self.fset is None:
            raise AttributeError(f"`get` will block until the next value is set, but no setter is defined.")

        async with condition:
            await condition.wait_for(predicate)
            return self.fget(obj)

    @t.overload
    def __get__(self, instance: None, owner: t.Type[t.Any] | None = ...) -> condition_property[_T]:
        ...

    @t.overload
    def __get__(self, instance: object, owner: t.Type[t.Any] | None = ...) -> accessor[_T]:
        ...

    def __get__(self, instance: object | None, owner: t.Type[t.Any] | None = None) -> condition_property[_T] | accessor[
        _T]:
        return super().__get__(instance, owner)

    def getter(self: condition_property[_T], fget: t.Callable[[t.Any], _T]) -> condition_property[_T]:
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def setter(self: condition_property[_T], fset: t.Callable[[t.Any, _T], None]) -> condition_property[_T]:
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def deleter(self: condition_property[_T], fdel) -> condition_property[_T]:
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
