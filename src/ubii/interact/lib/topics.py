import abc
import asyncio
import logging
import typing as t
from asyncio import Task
from collections import UserDict
from contextlib import suppress, AsyncExitStack, asynccontextmanager
from fnmatch import fnmatch
from functools import wraps, lru_cache
from warnings import warn

import ubii.proto as ub
from ubii.interact.types.topics import ITopicClient, ITopic, ITopicStore, IDataConnection


class AsyncTopicClient(ITopicClient, abc.ABC):
    """
    This implementation of the TopicClient
    """

    def __init__(self, topic_store: ITopicStore = None, **kwargs):
        self.__task__ = None
        self.topic_store = topic_store or TopicStore(default_factory=AsyncBufferedTopic)
        super().__init__(**kwargs)

    @property
    @abc.abstractmethod
    def topic_connection(self) -> IDataConnection:
        ...

    @classmethod
    async def make_record(cls, stream: t.AsyncIterator[ub.TopicData]) -> t.AsyncIterator[ub.TopicDataRecord]:
        async for data in stream:
            if data.topic_data_record_list:
                for record in data.topic_data_record_list:
                    yield record
            elif data.topic_data_record:
                yield data.topic_data_record
            else:
                yield data.error

    @classmethod
    async def split_to_topics(cls, stream, store: 'ITopicStore', log=None):
        log = log or logging.getLogger()

        async for record in cls.make_record(stream):
            topics = store.match_topic(record.topic)
            log.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
            if not topics:
                raise RuntimeError(f"No topics found for record with topic {record.topic}")
            else:
                await asyncio.gather(*[topic.publish(record) for topic in topics or ()])

    def start_topic_handling(self):
        if not self.__task__:
            self.__task__ = asyncio.create_task(
                self.split_to_topics(stream=self.topic_connection.stream(), store=self.topic_store)
            )

        return self

    @asynccontextmanager
    async def _cleanup_on_error(self):
        stack: AsyncExitStack
        with AsyncExitStack() as stack:
            stack.push_async_exit(self)
            yield
            stack.pop_all()

    async def stop_topic_handling(self):
        if self.__task__:
            self.__task__.cancel()
            with suppress(asyncio.CancelledError):
                await self.__task__

    async def __aenter__(self):
        async with self._cleanup_on_error():
            instance = self.start_topic_handling()
        return instance

    async def __aexit__(self, *exc_details):
        await self.stop_topic_handling()


class AsyncBufferedTopic(ITopic[int], AsyncExitStack):
    def __init__(self, pattern):
        super().__init__()
        self._pattern = pattern
        self._buffer = None
        self._last_token = -1
        self._callback_tasks: t.Dict[int, Task] = {}
        self.published = asyncio.Condition()

    def _make_async_callback(self, sync_callback: ITopic.TopicDataConsumer):
        @wraps(sync_callback)
        async def _callback(record):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sync_callback, record)

        return _callback

    def _make_task(self, callback: ITopic.TopicDataConsumer):
        if not asyncio.iscoroutinefunction(callback):
            callback = self._make_async_callback(callback)

        async def _run():
            while True:
                async with self.published:
                    await self.published.wait()
                    await callback(self.buffer)

        return asyncio.create_task(_run(), name=callback.__name__)

    def _next_token(self):
        self._last_token += 1
        return self._last_token

    @lru_cache
    def register_callback(self, callback: ITopic.TopicDataConsumer) -> int:
        token = self._next_token()
        self.push_async_callback(lambda *exc_info: self.unregister_callback(token))
        self._callback_tasks[token] = self._make_task(callback)
        return token

    async def unregister_callback(self, token: int) -> bool:
        task = self._callback_tasks.pop(token, None)
        if task is None:
            warn(f"No callback for {token} found in {self}")
            return False

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        return True

    async def publish(self, record: ub.TopicDataRecord):
        async with self.published:
            self.buffer = record
            self.published.notify_all()

    @property
    def buffer(self) -> ub.TopicDataRecord:
        return self._buffer

    @buffer.setter
    def buffer(self, value):
        self._buffer = value

    async def get_data(self, timeout=None) -> ub.TopicDataRecord:
        async with self.published:
            await asyncio.wait_for(self.published.wait(), timeout=timeout)
            return self.buffer

    async def stream(self, timeout=None):
        with suppress(asyncio.TimeoutError):
            while True:
                record = await self.get_data(timeout=timeout)
                yield record

    def __str__(self):
        return f"Topic {self._pattern}"


class TopicStore(ITopicStore, UserDict[str, ITopic]):
    def match_topic(self, topic: str) -> t.Tuple[ITopic, ...]:
        return tuple(top for topic_pattern, top in self.data.items() if fnmatch(name=topic, pat=topic_pattern))

    def match_pattern(self, pattern: str) -> t.Tuple[ITopic, ...]:
        return tuple(top for topic_pattern, top in self.data.items() if fnmatch(name=topic_pattern, pat=pattern))

    def __init__(self, default_factory):
        super().__init__()
        self._default_factory = default_factory

    def __missing__(self, key):
        self.data[key] = self._default_factory(key)
