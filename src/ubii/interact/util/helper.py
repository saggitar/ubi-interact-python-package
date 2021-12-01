from __future__ import annotations
import asyncio
import typing as t
from asyncio import Task
from functools import cached_property, partial, wraps

T = t.TypeVar('T')


# noinspection PyPep8Naming
class task(cached_property):
    """
    Wraps a function to make sure that when the function is awaited, it is only running once.

    :param func: typically an async defined method / function
    :return: a callable returning the Task running `func`
    """
    class once(t.Callable):
        def __init__(self, value, instance=None, coro=None):
            self.value = value
            self.coro = coro
            self.__self__ = instance

        def __call__(self):
            return self.value

        def rerun(self):
            if self.coro is None or self.__self__ is None:
                raise NotImplementedError(f"rerun not allowed for {self}")

            return self.coro(self.__self__)

    def __init__(self, func):
        super().__init__(func)
        self.func = partial(self._task_create, self.func)

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        self.attrname = f"{self.attrname}__task"

    def __get__(self, instance, owner=None):
        value = super().__get__(instance, owner)
        return self.once(value, coro=self.func)

    def __call__(self):
        return self.func()

    def _task_create(self, func, *args):
        return asyncio.create_task(func(*args), name=f"{self.attrname} for {args}")