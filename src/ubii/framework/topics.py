from __future__ import annotations

import abc
import asyncio
import logging
import typing as t
from contextlib import asynccontextmanager
from fnmatch import fnmatch
from warnings import warn

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

import ubii.proto as ub
from . import util
from .util.typing import T_co as _T_co, T_contra as _T_contra, Protocol

__protobuf__ = ub.__protobuf__

_Buffer = t.TypeVar('_Buffer')
_Token = t.TypeVar('_Token')

log = logging.getLogger(__name__)


class Consumer(Protocol[_T_contra]):
    def __call__(self, value: _T_contra) -> t.Coroutine[t.Any, t.Any, None] | None: ...


class DataConnection(t.AsyncIterator[ub.TopicData]):
    """
    A DataConnection can be used to asynchronously iterate over received TopicData messages,
    and to send TopicData messages to the master node.
    """

    @abc.abstractmethod
    async def send(self, data: ub.TopicData): ...


class TopicCoroutine(util.CoroutineWrapper[t.Any, t.Any, None], t.Generic[_Buffer]):
    """
    A topic coroutine waits until a value is written to the topic and then runs it's callback.
    """

    def __init__(self, *,
                 shared_resource_accessor: util.accessor[_Buffer],
                 callback: Consumer[_Buffer]):
        self.__name__ = callback.__name__ if hasattr(callback, '__name__') else repr(callback)  # type: ignore
        self.accessor = shared_resource_accessor
        self.callback = util.make_async(callback)
        super().__init__(coroutine=self._run())

    async def _run(self):
        while True:
            value: _Buffer = await self.accessor.get()
            await self.callback(value)


class TopicDataBufferManager(t.Generic[_Buffer], abc.ABC):
    @property
    @abc.abstractmethod
    def buffer(self: Topic[_Buffer, _Token]) -> util.accessor[_Buffer]:
        ...


class Topic(t.AsyncIterator[_Buffer], TopicDataBufferManager[_Buffer], t.Generic[_Buffer, _Token], abc.ABC):
    """
    A Topic can be used to asynchronously iterate over TopicDataRecords that are published to the topic.
    It can also register (and unregister) callbacks to handle the published values in a background task.

    To "publish" a value to the topic locally use ``await topic.buffer.set(...)``. The Client Protocol makes sure that
    all TopicDataRecords received via the ``DataConnection`` get forwarded to matching topics in this way.
    One doesn't need to manually "publish" data (except for e.g. mocking a connection)

    Publishing TopicData to the master node has nothing to do with our local topic representation,
    instead use the
    :TODO:
    """
    __unique_key_attr__ = 'pattern'

    on_subscribers_change: OnSubscribersChange | None

    async def __anext__(self) -> _Buffer:
        return await self.buffer.get()

    def __init__(self: Topic[_Buffer, _Token],
                 pattern,
                 *,
                 token_factory: t.Callable[[], _Token],
                 task_nursery: util.TaskNursery | None = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.on_subscribers_change = None
        self.pattern = pattern
        self.task_nursery = task_nursery or util.TaskNursery(name=f"Task Nursery for {self}")
        self.token_factory = token_factory
        self.callback_tasks: t.Dict[_Token, asyncio.Task] = {}
        self._subscriber_count = 0

    @property
    def subscriber_count(self):
        return self._subscriber_count

    @subscriber_count.setter
    def subscriber_count(self, value):
        if self.on_subscribers_change:
            self.on_subscribers_change(self, (self.subscriber_count, value))

        self._subscriber_count = value

    @asynccontextmanager
    async def _wait_for_event(self):
        self.on_subscribers_change.event.clear()
        yield
        await self.on_subscribers_change.event.wait()

    async def add_subscriber(self):
        async with self._wait_for_event():
            self.subscriber_count += 1

    async def remove_subscriber(self):
        async with self._wait_for_event():
            self.subscriber_count -= 1

    async def remove_all_subscribers(self):
        async with self._wait_for_event():
            self.subscriber_count = 0

    def register_callback(self, callback: Consumer[_Buffer]) -> _Token:
        """
        Register a callback function as a callback on this topic.

        This starts a task which waits for the ``buffer`` to be written to, and then executes the callback
        function on the value provided by the ``buffer`` accessor.

        If the callback is not a coroutine function, it will be run in the default executor pool (see
        https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools)

        This method returns a unique identifier (supplied by the ``token factory``) which can be used to deregister
        the callback at a later time. Registering a callback multiple times will make the
        callback run multiple times for each published value. One needs to keep the token around to later deregister
        (as you don't get the same token again by calling ``register_callback`` with the same callback).

        Also make sure you read the documentation on the ``task_manager`` attribute.

        :param callback: some callable which acts on a TopicDataRecord
        :return: a unique token (supplied by the ``token_factory``) to deregister the callback later
        """
        token = self.token_factory()
        self.callback_tasks[token] = self.task_nursery.create_task(
            TopicCoroutine(shared_resource_accessor=self.buffer, callback=callback)
        )
        return token

    async def unregister_callback(self, token: _Token, timeout=None) -> bool:
        """
        Unregister a callback with the unique token from the registration.
        Also cancel the callback task.
        Returns True if operation succeeded, False if no callback for the token is registered.
        If ``timeout`` is set, raise a ``TimeoutError`` if the task does not complete (cancel) in time
        (default behaviour: block until task is canceled successfully)

        :param timeout: don't wait indefinitely for task to cancel
        :param token: get it from ``register_callback``
        :return: boolean indicating success
        """
        task = self.callback_tasks.pop(token, None)
        if task is None:
            warn(f"No callback for {token} found in {self}")
            return False

        await asyncio.wait(self.task_nursery.stop_task(task), timeout=timeout)
        return True


class BasicTopic(Topic[ub.TopicDataRecord, int]):
    class integer_token_factory:
        """ creates increasing integers"""
        __last_token__ = -1

        def __call__(self):
            self.__last_token__ += 1
            return self.__last_token__

    def __init__(self, pattern, *, task_nursery: util.TaskNursery, **kwargs) -> None:
        super().__init__(pattern, token_factory=self.integer_token_factory(), task_nursery=task_nursery, **kwargs)
        self._buffer: ub.TopicDataRecord | None = None

    @util.hook
    def _set_buffer(self, value):
        self._buffer = value

    @util.hook
    def _get_buffer(self):
        return self._buffer

    @cached_property
    def buffer(self) -> util.accessor[ub.TopicDataRecord]:
        return util.accessor(funcs=(self._get_buffer, self._set_buffer))


class MatchMapping(t.Mapping[str, _T_co], abc.ABC):

    def match_name(self, name) -> t.Tuple[_T_co, ...]:
        """
        Returns all values where ``name`` matches the keys of contained values interpreted as a glob pattern.
        See documentation of ``fnmatch`` for more info.

        Example:

            container = MatchContainer({"foo": 1, "foo*": 2, "bar": 3})
            val = container.match_name('foo')
            print(val)

            >> (1, 2)

        """
        return tuple(val for pattern, val in self.items() if fnmatch(name=name, pat=pattern))

    def match_pattern(self, pattern) -> t.Tuple[_T_co, ...]:
        """
        Returns all values where the keys of contained values match the pattern (some glob pattern).
        See documentation of ``fnmatch`` for more info.

        Example:

            container = MatchContainer({"foo": 1, "foo*": 2, "bar": 3})
            val = container.match_pattern('foo')
            print(val)

            >> (1)

        """
        return tuple(top for topic_pattern, top in self.items() if fnmatch(name=topic_pattern, pat=pattern))


_Topic_co = t.TypeVar('_Topic_co', bound=Topic, covariant=True)


class TopicStore(MatchMapping[_Topic_co]):
    def __init__(self: TopicStore[_Topic_co], default_factory: t.Callable[[str], _Topic_co]):
        self._default_factory = default_factory
        self.data: t.Dict[str, _Topic_co] = {}

    @util.hook
    def create_topic(self, key):
        self.data[key] = self._default_factory(key)

    def __getitem__(self, key: str) -> _Topic_co:
        if key not in self.data:
            self.create_topic(key)
        assert key in self.data
        return self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> t.Iterator[str]:
        return iter(self.data)

    def __contains__(self, item):
        return item in self.data

    def __delitem__(self, key):
        if key not in self.data:
            raise KeyError(f"Can't delete item with key {key}")

        del self.data[key]


class StreamSplitRoutine(util.CoroutineWrapper[t.Any, t.Any, None]):
    """
    A StreamSplitRoutine splits TopicDataRecords form a TopicData to the buffers of topics from a TopicStore container
    (letting the TopicStore compute the matching topics for the topic of the record and then setting the buffer with
    the record)

    Of course this only works for Topics with TopicDataRecord buffers. If one designs a fancy topic with different
    buffer type, one has to also adjust the StreamSplitRoutine.
    """

    def __init__(self, *,
                 stream: t.AsyncIterator[ub.TopicData],
                 container: TopicStore[Topic[ub.TopicDataRecord, t.Any]],
                 logger: logging.Logger | None = None):

        self._stream = stream
        self._container = container
        self._logger = logger or logging.getLogger(__name__)
        super().__init__(coroutine=self.split_to_topics())

    async def make_record(self) -> t.AsyncIterator[ub.TopicDataRecord]:
        async for data in self._stream:
            if data.topic_data_record_list:
                for record in data.topic_data_record_list.elements:
                    yield record
            elif data.topic_data_record:
                yield data.topic_data_record
            else:
                raise data.error

    async def split_to_topics(self):
        async for record in self.make_record():
            topics = self._container.match_name(record.topic)
            self._logger.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
            if not topics:
                topics = (self._container[record.topic],)
                log.warning(f"No topics found for record with topic {record.topic}")

            await asyncio.gather(*[topic.buffer.set(record) for topic in topics or ()])


class OnSubscribeCallback(Protocol):
    async def __call__(self, client_id: str, *topic_patterns: str, as_regex: bool = ..., unsubscribe: bool = ...): ...


class OnSubscribersChange:
    def __init__(self,
                 client_id,
                 as_regex,
                 callback: OnSubscribeCallback):
        self.as_regex = as_regex
        self.client_id = client_id
        self.event = asyncio.Event()
        self.callback = callback

    def __call__(self, topic: Topic, change: t.Tuple[int, int]):
        old, new = change

        if new == 0 and old > 0:
            topic.task_nursery.create_task(
                self.callback(self.client_id, topic.pattern, as_regex=self.as_regex, unsubscribe=True)
            )

        if new == 1 and old < 1:
            topic.task_nursery.create_task(
                self.callback(self.client_id, topic.pattern, as_regex=self.as_regex, unsubscribe=False)
            )

        self.event.set()


class TopicMuxer(TopicDataBufferManager, ub.TopicMux, metaclass=ub.ProtoMeta):
    def _set_buffer(self, value):
        pass

    def _get_buffer(self):
        pass

    @cached_property
    def buffer(self: Topic[_Buffer, _Token]) -> util.accessor[_Buffer]:
        pass

    def __init__(self, mapping, **kwargs) -> None:
        super().__init__(mapping=mapping, **kwargs)


class TopicDemuxer(TopicDataBufferManager, ub.TopicDemux, metaclass=ub.ProtoMeta):
    def __init__(self, mapping, **kwargs) -> None:
        super().__init__(mapping=mapping, **kwargs)
