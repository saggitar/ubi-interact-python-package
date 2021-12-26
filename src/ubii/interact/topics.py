import abc
import asyncio
import logging
import typing as t
from collections import UserDict
from contextlib import AsyncExitStack, suppress
from fnmatch import fnmatch
from functools import cache

from _warnings import warn

from ubii import proto as ub
from ubii.interact import util
from ubii.interact.util import CoroutineWrapper


class DataConnection(t.AsyncIterator[ub.TopicData]):
    """
    A DataConnection can be used to asynchronously iterate over received TopicData messages,
    and to send TopicData messages to the master node.
    """

    @abc.abstractmethod
    async def send(self, data: ub.TopicData): ...


T = t.TypeVar('T')


class TopicCoroutine(t.Generic[T], CoroutineWrapper[t.Any, t.Any, None]):
    """
    A topic coroutine waits until a value is written to the topic and then runs it's callback.
    """

    def __init__(self, *,
                 shared_resource_accessor: util.accessor[T],
                 callback: t.Callable[[T], t.Optional[t.Coroutine[t.Any, t.Any, None]]]):
        self._get = shared_resource_accessor.next
        self._set = shared_resource_accessor.set
        if not asyncio.iscoroutinefunction(callback):
            callback = util.make_async(callback)

        self._callback: t.Callable[[T], t.Coroutine[t.Any, t.Any, None]] = callback
        super().__init__(coroutine=self._run())

    async def _run(self):
        while True:
            value: T = await self._get()
            await self._callback(value)


T_Token = t.TypeVar('T_Token')
T_Buffer = t.TypeVar('T_Buffer')


class Topic(t.AsyncIterator[ub.TopicDataRecord], t.Generic[T_Token, T_Buffer]):
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
    TopicDataConsumer = t.Callable[[T_Buffer], None]
    TokenFactory = t.Callable[[], T_Token]

    class default_token_factory(TokenFactory[int]):
        """ creates increasing integers. wow."""
        __last_token__ = -1

        def __call__(self):
            self.__last_token__ += 1
            return self.__last_token__

    async def __anext__(self) -> T_Buffer:
        # (see e.g. https://youtrack.jetbrains.com/issue/PY-38030)
        return self.buffer.next()  # noqa

    @t.overload
    def __init__(self: 'Topic[int, T_Buffer]', *,
                 token_factory: None,
                 task_manager: AsyncExitStack = None) -> None: ...

    @t.overload
    def __init__(self: 'Topic[T_Token, T_Buffer]', *,
                 token_factory: TokenFactory[T_Token],
                 task_manager: AsyncExitStack = None) -> None: ...

    def __init__(self, *,
                 token_factory: t.Optional[TokenFactory] = None,
                 task_manager: AsyncExitStack = None) -> None:
        token_factory = token_factory or self.default_token_factory()
        self._next_token = lambda: token_factory()
        self._exit_stack = task_manager or AsyncExitStack()
        self._callback_tasks = {}
        self._buffer = None

    @util.condition_property
    def buffer(self) -> T_Buffer:
        """
        e.g. a TopicDataRecord
        """
        return self._buffer

    @buffer.setter
    def buffer(self, value: T_Buffer):
        self._buffer = value

    @cache
    def register_callback(self, callback: TopicDataConsumer) -> T:
        """
        Register a callback function as a callback on this topic.

        This starts a task which waits for the ``buffer`` to be written to, and then executes the callback
        function on the value provided by the ``buffer`` accessor. In the default implementation the buffer contains
        a single ``TopicDataRecord``, so the callable needs to match the type alias ``TopicDataConsumer``.

        If you want to design topics with a record history, or some other functionality which changes the type of values
        in the buffer, make sure to adjust the ``TopicDataConsumer`` class attribute in your derived class, or the
        type checker will be confused (which is ok to test, but not good in the long run)

        If the callback is not a coroutine function, it will be run in the default executor pool (see
        https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools)

        This method returns a unique identifier (supplied by the ``token factory``) which can be used to deregister
        the callback at a later time. Tokens are cached, so registering a callback multiple times will not make the
        callback run multiple times for each published value and instead return the token from the first registration.

        Although this means you don't necessarily need to keep the token around to later deregister (as you can get the same
        token again by calling ``register_callback`` with the same callback), that's probably not a good idea.

        Also make sure you read the documentation on the ``task_manager`` attribute.

        :param callback: some callable which acts on a TopicDataRecord
        :return: a unique token (supplied by the ``token_factory``) to deregister the callback later
        """
        token = self._next_token()
        self._exit_stack.push_async_callback(lambda *exc_info: self.unregister_callback(token))
        self._callback_tasks[token] = asyncio.create_task(
            TopicCoroutine(shared_resource_accessor=self.buffer, callback=callback)  # noqa
            # (see e.g. https://youtrack.jetbrains.com/issue/PY-38030)
        )
        return token

    async def unregister_callback(self, token: T_Token, timeout=None) -> bool:
        """
        Unregister a callback with the unique token from the registration.
        Also the callback task. Returns True if operation succeeded, False if no callback for the token is registered.
        If ``timeout`` is set, raise a ``TimeoutError`` if the task does not complete in time (default behaviour: block
        until task is canceled successfully)

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
            await asyncio.wait(task, timeout=timeout)
        return True

    @property
    def task_manager(self) -> AsyncExitStack:
        """
        This context manager unregisters the callbacks and stops the tasks on exit.
        Setting this values transfers handling of all callbacks to the new AsyncExitStack.

        :return: a exit stack context manager
        """
        return self._exit_stack

    @task_manager.setter
    def task_manager(self, manager: AsyncExitStack):
        """
        make temporary stack (so exiting the previous one does not unregister) and close it when the new context exits.
        """
        manager.push_async_exit(self._exit_stack.pop_all())
        self._exit_stack = manager


class TopicContainer(UserDict[str, Topic], t.Generic[T_Buffer]):
    def match_topic(self: 'TopicContainer[T_Buffer]', topic) -> t.Tuple[Topic[T_Buffer], ...]:
        return tuple(top for topic_pattern, top in self.data.items() if fnmatch(name=topic, pat=topic_pattern))

    def match_pattern(self: 'TopicContainer[T_Buffer]', pattern) -> t.Tuple[Topic[T_Buffer], ...]:
        return tuple(top for topic_pattern, top in self.data.items() if fnmatch(name=topic_pattern, pat=pattern))

    def __init__(self: 'TopicContainer[T_Buffer]', default_factory: t.Callable[[str], Topic[T_Buffer]]):
        super().__init__()
        self._default_factory = default_factory

    def __missing__(self, key):
        self.data[key] = self._default_factory(key)


class StreamSplitRoutine(CoroutineWrapper[t.Any, t.Any, None]):
    """
    A StreamSplitRoutine splits TopicDataRecords form a TopicData to the buffers of topics from a TopicContainer
    (letting the TopicContainer compute the matching topics for the topic of the record and then setting the buffer with
    the record)

    Of course this only works for Topics with TopicDataRecord buffers. If one designs a fancy topic with different
    buffer type, one has to also adjust the StreamSplitRoutine.
    """

    def __init__(self, *,
                 stream: t.AsyncIterator[ub.TopicData],
                 container: TopicContainer[ub.TopicDataRecord],
                 logger: logging.Logger = None):

        self._stream = stream
        self._container = container
        self._logger = logger or logging.getLogger(__name__)
        super().__init__(coroutine=self._split_to_topics())

    async def _make_record(self) -> t.AsyncIterator[ub.TopicDataRecord]:
        async for data in self._stream:
            if data.topic_data_record_list:
                for record in data.topic_data_record_list:
                    yield record
            elif data.topic_data_record:
                yield data.topic_data_record
            else:
                yield data.error

    async def _split_to_topics(self):
        async for record in self._make_record():
            topics = self._container.match_topic(record.topic)
            self._logger.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
            if not topics:
                raise RuntimeError(f"No topics found for record with topic {record.topic}")
            else:
                await asyncio.gather(*[topic.buffer.set(record) for topic in topics or ()])

    def __await__(self):
        return self._coroutine.__await__()
