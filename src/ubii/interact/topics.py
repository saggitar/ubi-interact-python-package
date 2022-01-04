from __future__ import annotations

import abc
import asyncio
import logging
import typing as t
from contextlib import AsyncExitStack, suppress
from fnmatch import fnmatch
from functools import cached_property
from warnings import warn

import codestare.async_utils as util
import ubii.proto as ub

_Buffer = t.TypeVar('_Buffer')
_Token = t.TypeVar('_Token')
_T_contra = t.TypeVar('_T_contra', contravariant=True)
_T_co = t.TypeVar('_T_co', covariant=True)


class Consumer(t.Protocol[_T_contra]):
    def __call__(self, value: _T_contra) -> t.Coroutine[t.Any, t.Any, None] | None: ...


class DataConnection(t.AsyncIterator[ub.TopicData]):
    """
    A DataConnection can be used to asynchronously iterate over received TopicData messages,
    and to send TopicData messages to the master node.
    """

    @abc.abstractmethod
    async def send(self, data: ub.TopicData): ...


class TopicCoroutine(t.Generic[_Buffer], util.wrapper.CoroutineWrapper[t.Any, t.Any, None]):
    """
    A topic coroutine waits until a value is written to the topic and then runs it's callback.
    """

    def __init__(self, *,
                 shared_resource_accessor: util.accessor[_Buffer],
                 callback: Consumer[_Buffer]):
        self._get = shared_resource_accessor.get
        self._set = shared_resource_accessor.set
        self._callback = util.helper.make_async(callback)
        super().__init__(coroutine=self._run())

    async def _run(self):
        while True:
            value: _Buffer = await self._get()
            await self._callback(value)


class Topic(t.Generic[_Buffer, _Token], abc.ABC):
    """
    A Topic can be used to asynchronously iterate over TopicDataRecords that are published to the topic.
    It can also register (and unregister) callbacks to handle the published values in a background task.

    To "publish" a value to the topic locally use ``await topic.buffer.set(...)``. The used Protocol makes sure that
    all TopicDataRecords received via the ``DataConnection`` get forwarded to matching topics in this way.
    One doesn't need to manually "publish" data (except for e.g. mocking a connection)

    Publishing TopicData to the master node has nothing to do with our local topic representation,
    instead use
    :TODO:
    """

    @property
    @abc.abstractmethod
    def buffer(self: Topic[_Buffer, _Token]) -> util.accessor[_Buffer]: ...

    class default_token_factory:
        """ creates increasing integers. wow."""
        __last_token__ = -1

        def __call__(self):
            self.__last_token__ += 1
            return self.__last_token__

    async def __anext__(self) -> _Buffer:
        return await self.buffer.get()

    @t.overload
    def __init__(self: Topic[_Buffer, int], *,
                 token_factory: None = ...,
                 task_manager: AsyncExitStack | None = ...) -> None:
        ...

    @t.overload
    def __init__(self: Topic[_Buffer, _Token], *,
                 token_factory: t.Callable[[], _Token] = ...,
                 task_manager: AsyncExitStack | None = ...) -> None:
        ...

    def __init__(self, *,
                 token_factory: t.Callable[[], _Token] | None = None,
                 task_manager: AsyncExitStack | None = None) -> None:
        token_factory = token_factory or self.default_token_factory()
        self._next_token = lambda: token_factory()
        self._exit_Stack = task_manager or AsyncExitStack()
        self._callback_tasks: t.Dict[_Token, asyncio.Task] = {}

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
        token = self._next_token()
        self._exit_Stack.push_async_callback(lambda *exc_info: self.unregister_callback(token))
        self._callback_tasks[token] = asyncio.create_task(
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
        task = self._callback_tasks.pop(token, None)
        if task is None:
            warn(f"No callback for {token} found in {self}")
            return False

        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.wait([task], timeout=timeout)
        return True

    @property
    def task_manager(self) -> AsyncExitStack:
        """
        This context manager unregisters the callbacks and stops the tasks on exit.
        Setting this values transfers handling of all callbacks to the new AsyncExitStack.

        :return: a exit stack context manager
        """
        return self._exit_Stack

    @task_manager.setter
    def task_manager(self, manager: AsyncExitStack):
        """
        make temporary stack (so exiting the previous one does not unregister) and close it when the new context exits.
        """
        manager.push_async_exit(self._exit_Stack.pop_all())
        self._exit_Stack = manager


class DefaultTopic(Topic[ub.TopicDataRecord, int]):
    def __init__(self: Topic[ub.TopicDataRecord, int], *, task_manager: AsyncExitStack | None = None) -> None:
        super().__init__(token_factory=None, task_manager=task_manager)

    @cached_property
    def buffer(self) -> util.accessor[ub.TopicDataRecord]:
        return util.accessor[ub.TopicDataRecord]()


class MatchMixin:
    def match_name(self: t.Mapping[str, _T_co], name) -> t.Tuple[_T_co, ...]:
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

    def match_pattern(self: t.Mapping[str, _T_co], pattern) -> t.Tuple[_T_co, ...]:
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


class TopicStore(t.Mapping[str, _Topic_co], MatchMixin, t.Generic[_Topic_co]):
    def __init__(self: TopicStore[_Topic_co], default_factory: t.Callable[[], _Topic_co]):
        self._default_factory = default_factory
        self.data = {}

    def __getitem__(self, key: str) -> _Topic_co:
        return self.data.setdefault(key, self._default_factory())

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> t.Iterator[_Topic_co]:
        return iter(self.data)

    def __contains__(self, item):
        return item in self.data


class StreamSplitRoutine(util.wrapper.CoroutineWrapper[t.Any, t.Any, None]):
    """
    A StreamSplitRoutine splits TopicDataRecords form a TopicData to the buffers of topics from a TopicStore container
    (letting the TopicStorethe matching topics for the topic of the record and then setting the buffer with
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
                warn(f"No topics found for record with topic {record.topic}")

            await asyncio.gather(*[topic.buffer.set(record) for topic in topics or ()])
