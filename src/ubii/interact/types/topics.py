import asyncio
import typing as t
from abc import ABC, abstractmethod
from asyncio import Task
from collections import defaultdict
from contextlib import suppress
from fnmatch import fnmatch
from functools import wraps
from warnings import warn
from .meta import InitContextManager as _InitContextManager

from ubii.proto import (
    TopicData,
    TopicDataRecord,
)


class IDataConnection(ABC):
    @property
    @abstractmethod
    def stream(self) -> t.AsyncGenerator[TopicData, None]: ...

    @abstractmethod
    async def asend(self, data: TopicData): ...

    @abstractmethod
    async def initialize(self) -> t.AsyncContextManager['IDataConnection']: ...


class TopicStore(ABC, _InitContextManager):
    class Topic:
        TopicDataConsumer = t.Callable[[TopicDataRecord], None]

        class Token:
            __id__ = -1

            @classmethod
            def create(cls):
                cls.__id__ += 1
                return cls.__id__

        def __init__(self, pattern):
            self._pattern = pattern
            self._buffer = None
            self.callbacks: t.Dict[int, Task] = {}
            self.published = asyncio.Condition()

        def _make_async_callback(self, sync_callback: TopicDataConsumer):
            @wraps(sync_callback)
            async def _callback(record):
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, sync_callback, record)

            return _callback

        def _make_task(self, callback: TopicDataConsumer):
            if not asyncio.iscoroutinefunction(callback):
                callback = self._make_async_callback(callback)

            async def _run():
                while True:
                    async with self.published:
                        await self.published.wait()
                        await callback(self.buffer)

            return asyncio.create_task(_run(), name=callback.__name__)

        def register_callback(self, callback: TopicDataConsumer) -> int:
            token = self.Token.create()
            self.callbacks[token] = self._make_task(callback)
            return token

        def unregister_callback(self, token: int) -> bool:
            removed = self.callbacks.pop(token, None)
            if removed is None:
                warn(f"No callback for {token} found in {self}")
                return False

            return True

        async def publish(self, record: TopicDataRecord):
            async with self.published:
                self.buffer = record
                self.published.notify_all()

        @property
        def buffer(self) -> TopicDataRecord:
            return self._buffer

        @buffer.setter
        def buffer(self, value):
            self._buffer = value

        async def get_data(self) -> TopicDataRecord:
            async with self.published:
                await self.published.wait()
                return self.buffer

        async def stream(self, timeout=None):
            with suppress(asyncio.exceptions.TimeoutError):
                while True:
                    record = await asyncio.wait_for(self.get_data(), timeout=timeout)
                    yield record

        def __str__(self):
            return f"Topic {self._pattern}"

    def __init__(self):
        self.data = {}

    def __getitem__(self, topic: str) -> Topic:
        return self.data.setdefault(topic, self.Topic(pattern=topic))

    def values(self) -> t.Iterable[Topic]:
        return self.data.values()

    def __contains__(self, topic: str) -> bool:
        return topic in self.data

    def matching(self, topic: str = None, pattern=None) -> t.Tuple[Topic, ...]:
        if topic is None and pattern is None:
            raise ValueError("You need to specify `topic` or `pattern`")
        if topic is not None and pattern is not None:
            raise ValueError("You can't specify both `topic` and `pattern`")

        if topic:
            return tuple(top for topic_pattern, top in self.data.items()
                         if fnmatch(name=topic, pat=topic_pattern))
        if pattern:
            return tuple(top for topic_pattern, top in self.data.items()
                         if fnmatch(name=topic_pattern, pat=pattern))

    @_InitContextManager.init_ctx
    async def _handle_topic_callback_tasks(self):
        yield
        for topic in self.values():
            for task in topic.callbacks.values():
                task.cancel()
