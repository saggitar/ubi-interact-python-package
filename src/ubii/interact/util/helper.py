from __future__ import annotations
import asyncio
import typing as t
from asyncio import Task
from functools import wraps

T = t.TypeVar('T')


# noinspection PyPep8Naming
class once(t.Callable[[t.Any], t.Awaitable[T]]):
    """
    Wraps a function using the async await syntax and returns a future.
    This makes sure that when the function is awaited, it is only running once.

    :param func: typically an async defined method / function
    :return: a callable returning the Task running `func`
    """
    def __init__(self, func: t.Callable[..., t.Coroutine[t.Any, t.Any, T]]):
        self.__func__ = func
        self.__self__ = None
        self.task: Task[T] | None = None
        wraps(func)(self)

    def __call__(self, *args, **kwargs) -> Task[T]:
        if self.__self__:
            args = (self.__self__, ) + args

        if not self.task:
            self.task = asyncio.create_task(self.__func__(*args, **kwargs), name=str(self.__func__))
        return self.task

    def __get__(self, instance, owner):
        self.__self__ = instance
        return self

    async def rerun(self, *args, **kwargs) -> t.Coroutine[t.Any, t.Any, T]:
        if self.__self__:
            args = (self.__self__, ) + args

        return self.__func__(*args, **kwargs)

    def __new__(cls, *args: t.Callable[..., t.Coroutine[t.Any, t.Any, T]], **kwargs) -> once[T]:
        return super().__new__(cls, *args, **kwargs)