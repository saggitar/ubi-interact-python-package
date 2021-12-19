import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager
from functools import cached_property

log = logging.getLogger(__name__)

T_Instance = t.TypeVar('T_Instance', bound='InitContextManager')


class Initializable:
    @cached_property
    def init_lock(self):
        return asyncio.Lock()

    async def init_logic(self: T_Instance) -> t.AsyncIterator[T_Instance]:
        if self._lock.locked():
            yield self
        else:
            async with self._lock:
                yield self

    @asynccontextmanager
    async def initialize(self):
        async for instance in self.init_logic():
            yield instance
