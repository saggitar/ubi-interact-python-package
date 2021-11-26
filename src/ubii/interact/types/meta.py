import asyncio
from collections import defaultdict, UserDict
from contextlib import asynccontextmanager, AsyncExitStack
from functools import wraps, partial
from typing import Dict, Set, TypeVar, Generator, Any

_C = TypeVar('_C', bound='InitContextManager')


# noinspection PyPep8Naming
class InitContextManager:
    class _contexts(UserDict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.frozen = False

        def __setitem__(self, key, value):
            if self.frozen:
                raise RuntimeError(f"You can't register an new context after the first call to `initialize`")
            else:
                self.data[key] = value

    _registered_contexts = _contexts()
    _stacks: Dict[int, AsyncExitStack] = {}

    @asynccontextmanager
    async def initialize(self: _C) -> Generator[_C, Any, None]:
        InitContextManager._registered_contexts.frozen = True
        key = id(self)
        stack: AsyncExitStack = InitContextManager._stacks.get(key)
        if stack:
            async with stack:
                yield self
        else:
            stack = InitContextManager._stacks[key] = AsyncExitStack()
            async with stack as ctx:
                for t, contexts in InitContextManager._registered_contexts.items():
                    if not isinstance(self, t):
                        continue
                    for cm in contexts:
                        await ctx.enter_async_context(cm(self))
                yield self

    class _init_ctx:
        def __init__(self, func, owner=None):
            self.ctx_manager = asynccontextmanager(func)
            self.name = None
            self.owner = owner
            wraps(func)(self)

        def __set_name__(self, owner, name):
            if self.owner and not issubclass(owner, self.owner):
                raise TypeError(f"Type mismatch when setting {owner}.{name}. "
                                f"{owner} if not a subclass of {self.owner}")

            self.owner = owner
            self.name = name
            InitContextManager._registered_contexts.setdefault(owner, set()).add(self)

        def __call__(self, instance):
            return self.ctx_manager(instance)

        def __str__(self):
            return self.name

        def __repr__(self):
            return f"<init_ctx for {self.owner}.{self.name or '__unknown__'}>"

    @classmethod
    def init_ctx(cls, func):
        return cls._init_ctx(func, owner=cls)

    def __del__(self):
        # id is only guaranteed to be unique for the lifetime of the object
        del InitContextManager._stacks[id(self)]
        pass
