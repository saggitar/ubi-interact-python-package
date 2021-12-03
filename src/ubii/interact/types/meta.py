import asyncio
import logging
import typing as t
from collections import UserDict
from contextlib import asynccontextmanager, AsyncExitStack
from functools import wraps, partial, cached_property, lru_cache

from itertools import chain

_C = t.TypeVar('_C', bound='InitContextManager')

log = logging.getLogger(__name__)

# noinspection PyPep8Naming
class InitContextManager:
    class _contexts(UserDict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.frozen = {}

        def __setitem__(self, key, value):
            if self.frozen.get(key):
                raise RuntimeError(f"You can't register an new context for {key} after the first call to `initialize`")
            else:
                self.data[key] = value

    _registered_contexts = _contexts()

    @cached_property
    def _init_ctx_stack(self):
        return AsyncExitStack()

    @cached_property
    def _init_ctx_lock(self):
        return asyncio.Lock()

    @asynccontextmanager
    async def initialize(self: _C) -> t.Generator[_C, t.Any, None]:
        self._registered_contexts.frozen.setdefault(type(self), True)

        lock = self._init_ctx_lock
        if lock.locked():
            yield self
        else:
            async with lock:
                stack = self._init_ctx_stack
                async with stack as ctx:
                    log.debug(f"acquiring {self.__class__.__name__}")
                    contexts = chain.from_iterable(init_ctx
                                                   for type_, init_ctx in self._registered_contexts.items()
                                                   if isinstance(self, type_))
                    for init_ctx in sorted(contexts, key=lambda c: c.priority, reverse=True):
                        await ctx.enter_async_context(init_ctx(self))

                    yield self
                    log.debug(f"releasing {self.__class__.__name__}")

    class _init_ctx:
        def __init__(self, func, owner=None, priority=0):
            self.ctx_manager = asynccontextmanager(func)
            self.name = None
            self.owner: t.Type[InitContextManager] = owner
            self.priority = priority
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
    def init_ctx(cls, fun=None, priority=None):
        if fun is None:
            return partial(cls._init_ctx, owner=cls, priority=priority or 0)
        else:
            return cls._init_ctx(func=fun, owner=cls, priority=priority or 0)
