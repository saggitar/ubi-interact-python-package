"""
This module provides some classes to implement the handling of the :class:`ubii.proto.TopicData` messages
which are send by the master node when the client subscribes to a topic via a
:class:`ubii.framework.services.ServiceCall`.

The client node will need a :class:`DataConnection` to the *master node*, e.g. a websocket connection. Some example
connections are implemented in :mod:`ubii.framework.connections`. At which point during its lifetime the client is
able to establish this :class:`DataConnection` is described in greater detail in the :mod:`ubii.node.protocol` module.

After the :class:`DataConnection` is established, messages can be read using the async interator API and send
using the :meth:`DataConnection.send` method.

Note:

    The JS API currently defines methods to directly add a callback for a topic pattern -- to do the same in the Python API
    you first get a reference to the specific :class:`Topic` using the pattern, then add the callback with the topics
    :meth:`~Topic.register_callback` method. ::

        import logging
        from ubii.node import connect_client
        from ubii.framework.client import Subscriptions

        log = logging.getLogger('MyLogger')

        async def main():
            async with connect_client() as client:
                topic, = await client[Subscriptions].subscribe_topic('foo/bar') # get a topic reference
                topic.register_callback(log.info) # add callback

See Also:
    :class:`TopicStore` -- details how topics are retrieved by pattern
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import fnmatch
import logging
import typing
import warnings

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

import ubii.proto
from . import util
from .util.typing import (
    T_co,
    T_contra,
    Protocol,
)

__protobuf__ = ubii.proto.__protobuf__

T_Buffer = typing.TypeVar('T_Buffer')
T_Token = typing.TypeVar('T_Token')

log = logging.getLogger(__name__)


class Consumer(Protocol[T_contra]):
    def __call__(self, value: T_contra) -> typing.Coroutine[typing.Any, typing.Any, None] | None:
        """
        Consumer objects need to have this call signature

        Args:
            value: something to consume

        Returns:
            no return or coroutine with no return
        """


class DataConnection(typing.AsyncIterator[ubii.proto.TopicData]):
    """
    A DataConnection can be used to asynchronously iterate over received TopicData messages,
    and to send TopicData messages to the master node.
    """

    @abc.abstractmethod
    async def send(self, data: ubii.proto.TopicData): ...


class TopicCoroutine(util.CoroutineWrapper[typing.Any, typing.Any, None], typing.Generic[T_Buffer]):
    """
    A topic coroutine waits until a value is written to the topic and then runs its callback.
    """

    def __init__(self, *,
                 shared_resource_accessor: util.accessor[T_Buffer],
                 callback: Consumer[T_Buffer]):
        """
        Args:
            shared_resource_accessor: e.g. the buffer of a topic
            callback: a callable consuming the buffer values
        """
        self.__name__ = callback.__name__ if hasattr(callback, '__name__') else repr(callback)  # type: ignore
        self.accessor = shared_resource_accessor
        self.callback = util.make_async(callback)
        super().__init__(coroutine=self._run())

    async def _run(self):
        while True:
            value: T_Buffer = await self.accessor.get()
            await self.callback(value)


class TopicDataBufferManager(typing.Generic[T_Buffer], abc.ABC):
    """
    Simple :class:`~abc.ABC` to make sure the inheriting class has a buffer field
    """

    @property
    @abc.abstractmethod
    def buffer(self: Topic[T_Buffer, T_Token]) -> util.accessor[T_Buffer]:
        """
        Use this attribute to synchronize access to the managed resource
        """

@util.dunder.repr('pattern', 'token_factory', 'task_nursery')
class Topic(typing.AsyncIterator[T_Buffer], TopicDataBufferManager[T_Buffer], typing.Generic[T_Buffer, T_Token],
            abc.ABC):
    """
    A :class:`Topic` can be used to asynchronously iterate over :class:`TopicDataRecords <ubii.proto.TopicDataRecord>`
    which are published to the topic.
    It can also register (and unregister) callbacks to handle the published values in a background task.

    To "publish" a value to the topic locally use
    :meth:`await topic.buffer.set(...) <codestare.async_utils.descriptor.accessor.set>`.

    The :class:`Client Protocol <ubii.framwork.client.AbstractClientProtocol>` makes sure that all
    :class:`~ubii.proto.TopicDataRecord` messages received via the clients
    :class:`DataConnection` (created in its :attr:`~ubii.framwork.client.UbiiClient.Context`)
    get forwarded to matching topics in this way. One doesn't need to manually "publish" data
    except when e.g. mocking a connection.

    To publish TopicData to the master node instead use the :class:`~ubii.framework.client.Publish` behaviour of the
    :class:`~ubii.framework.client.UbiiClient`.

    Warning:

        The `master node` does not allow the clients to subscribe to the same topic multiple times. The application
        code on the other hand might want to "subscribe" to the topic, without caring about other subscriptions in other
        parts of the application. Implementing this kind of "subscription manager" can be easily done with the
        :attr:`.subscriber_count` property as well as the :meth:`.add_subscriber`, :meth:`remove_subscriber` and
        :meth:`remove_all_subscribers` methods, which trigger the :attr:`.on_subscribers_change` callback.
        E.g. set the actual service call as :attr:`.on_subscribers_change` callback of the topic and simply count
        calls to 'subscribe' and 'unsubscribe' from the topic by setting the virtual :attr:`.subscriber_count`.

    See Also:

        :class:`~ubii.node.protocol.LegacyProtocol.implement_subscriptions` -- example implementation of
        :class:`~ubii.framework.client.Subscriptions` behaviour using this module


    Attributes:
        buffer: inherited from :class:`TopicDataBufferManager`
    """
    on_subscribers_change: OnSubscribersChange | None

    async def __anext__(self) -> T_Buffer:
        return await self.buffer.get()

    def __init__(self: Topic[T_Buffer, T_Token],
                 pattern,
                 *,
                 token_factory: typing.Callable[[], T_Token],
                 task_nursery: util.TaskNursery | None = None,
                 **kwargs) -> None:
        """

        Args:
            pattern: initializes :attr:`.pattern`
            token_factory: initializes :attr:`.token_factory`
            task_nursery: initializes :attr:`.task_nursery`
            **kwargs: passed to super class init methods
        """
        super().__init__(**kwargs)
        self.on_subscribers_change = None
        """
        Special callback to execute when :attr:`.subscriber_count` changes -- defaults to :obj:`None`
        """
        self.pattern: str = pattern
        """
        Wildcard pattern or simple string defining the topic
        """
        self.task_nursery: util.TaskNursery = task_nursery or util.TaskNursery(name=f"Task Nursery for {self}")
        """
        takes care of managing tasks for callbacks
        """
        self.token_factory: typing.Callable[[], T_Token] = token_factory
        """
        called to create unique tokens to unregister callbacks, see :meth:`.register_callback`
        """
        self.callback_tasks: typing.Dict[T_Token, asyncio.Task] = {}
        """
        Mapping :math:`Token \\rightarrow Task` to access created callback tasks
        """
        self._subscriber_count = 0

    @property
    def subscriber_count(self):
        """
        number of virtual "subscribers" to this topic. Executes :attr:`.on_subscribers_changed` when it is
        set -- with args

            *   ``topic`` = this topic
            *   ``change`` = (old subscriber count, new subscriber count)
        """
        return self._subscriber_count

    @subscriber_count.setter
    def subscriber_count(self, value):
        if self.on_subscribers_change:
            self.on_subscribers_change(self, (self.subscriber_count, value))

        self._subscriber_count = value

    @contextlib.asynccontextmanager
    async def _wait_for_event(self):
        self.on_subscribers_change.event.clear()
        yield
        await self.on_subscribers_change.event.wait()

    async def add_subscriber(self):
        """
        Increase subscriber count and wait for :attr:`.on_subscriber_change` callback to finish
        """
        async with self._wait_for_event():
            self.subscriber_count += 1

    async def remove_subscriber(self):
        """
        Decrease subscriber count and wait for :attr:`.on_subscriber_change` callback to finish
        """
        async with self._wait_for_event():
            self.subscriber_count -= 1

    async def remove_all_subscribers(self):
        """
        Set subscriber count to ``0`` and wait for :attr:`.on_subscriber_change` callback to finish
        """
        async with self._wait_for_event():
            self.subscriber_count = 0

    def register_callback(self, callback: Consumer[T_Buffer]) -> T_Token:
        """
        Register a callback function as a callback on this topic.

        This starts a task which waits for the :attr:`.buffer` to be written to, and then executes the callback
        function on the value provided by the :attr:`.buffer` accessor.

        If the callback is not a coroutine function, it will be run in the default executor pool (see
        https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools)

        This method returns a unique identifier (supplied by the :attr:`.token_factory`) which can be used to deregister
        the callback at a later time. Registering a callback multiple times will make the
        callback run multiple times for each published value. One needs to keep the token around to later deregister
        (as you don't get the same token again by calling :meth:`register_callback` with the same callback).

        See Also:

            :attr:`task_manager` -- how to change who is responsible for executing callback tasks

        Args:
            callback: some callable which acts on a TopicDataRecord

        Returns:
            a unique token (supplied by the :attr:`token_factory`) to deregister the callback later
        """
        token = self.token_factory()
        self.callback_tasks[token] = self.task_nursery.create_task(
            TopicCoroutine(shared_resource_accessor=self.buffer, callback=callback)
        )
        return token

    async def unregister_callback(self, token: T_Token, timeout=None) -> bool:
        """
        Unregister a callback with the unique token from the registration.
        Also cancel the callback task.

        Args:
            timeout: don't wait indefinitely for task to cancel
            token: from :meth:`.register_callback`

        Returns:
            True if operation succeeded, False if no callback for the token is registered.

        Raises:
            TimeoutError: If ``timeout`` is set and the task does not complete (cancel) in time --
                default behaviour: block until task is canceled successfully
        """
        task = self.callback_tasks.pop(token, None)
        if task is None:
            warnings.warn(f"No callback for {token} found in {self}")
            return False

        await asyncio.wait(self.task_nursery.stop_task(task), timeout=timeout)
        return True


class BasicTopic(Topic[ubii.proto.TopicDataRecord, int]):
    """
    A :class:`Topic` with a :attr:`~Topic.token_factory` simply producing ascending integers and
    a simple :attr:`.buffer` for :class:`~ubii.proto.TopicDataRecord` messages
    """

    class integer_token_factory:
        """
        creates increasing integers ::

            >>> factory = integer_token_factory()
            >>> another_factory = integer_token_factory()
            >>> factory()
            0
            >>> another_factory()
            1
            >>> factory()
            2
            ...

        """
        __last_token__ = -1

        def __call__(self):
            self.__last_token__ += 1
            return self.__last_token__

    def __init__(self, pattern, **kwargs) -> None:
        super().__init__(pattern, token_factory=self.integer_token_factory(), **kwargs)
        self._buffer: ubii.proto.TopicDataRecord | None = None

    @util.hook
    def _set_buffer(self, value):
        self._buffer = value

    @util.hook
    def _get_buffer(self):
        return self._buffer

    @cached_property
    def buffer(self) -> util.accessor[ubii.proto.TopicDataRecord]:
        """
        You can use the :meth:`~codestare.async_utils.descriptor.accessor.get` and
        :meth:`~codestare.async_utils.descriptor.accessor.set` methods to synchronize access to the internal
        :class:`ubii.proto.TopicDataRecord` value.

        See Also:
           :class:`~codestare.async_utils.descriptor.accessor` -- details about access to shared resource

        """
        return util.accessor(funcs=(self._get_buffer, self._set_buffer))


class MatchMapping(typing.Mapping[str, T_co], abc.ABC):

    def match_name(self, name) -> typing.Tuple[T_co, ...]:
        """
        Returns all values where ``name`` matches the keys of contained values interpreted as a glob pattern.

        Example:

            >>> container = MatchContainer({"foo": 1, "foo*": 2, "bar": 3})
            >>> val = container.match_name('foo')
            >>> print(val)
            (1, 2)

        See Also:
            :mod:`fnmatch` -- details about glob patterns

        """
        return tuple(val for pattern, val in self.items() if fnmatch.fnmatch(name=name, pat=pattern))

    def match_pattern(self, pattern) -> typing.Tuple[T_co, ...]:
        """
        Returns all values where the keys of contained values match the glob ``pattern``.

        Example:

            >>> container = MatchContainer({"foo": 1, "foo*": 2, "bar": 3})
            >>> val = container.match_pattern('foo')
            >>> print(val)
            (1)

        See Also:
            :mod:`fnmatch` -- details about glob patterns

        """
        return tuple(top for topic_pattern, top in self.items() if fnmatch.fnmatch(name=topic_pattern, pat=pattern))


Topic_co = typing.TypeVar('Topic_co', bound=Topic, covariant=True)


class TopicStore(MatchMapping[Topic_co]):
    """
    A TopicStore acts like a :class:`~collections.defaultdict` mapping of :math:`pattern \\rightarrow Topic`, but allows for
    complex matching of keys using :meth:`.match_pattern` and :meth:`.match_name`

    Example:

        >>> from ubii.framework.topics import TopicStore
        >>> class Topic:
        ...     def __init__(self, pattern):
        ...             self.pattern = pattern
        ...
        >>> store = TopicStore(default_factory=Topic)
        >>> store.create_topic('topic/glob/pattern_one')
        >>> store.create_topic('topic/glob/pattern_two')
        >>> [topic.pattern for topic in store.match_pattern('topic*')]
        ['topic/glob/pattern_one', 'topic/glob/pattern_two'])

    """

    def __init__(self: TopicStore[Topic_co], default_factory: typing.Callable[[str], Topic_co]):
        self._default_factory = default_factory
        self.data: typing.Dict[str, Topic_co] = {}

    @util.hook
    @util.document_decorator(util.hook)
    def create_topic(self, key: str) -> None:
        """
        Called whenever a key is not present in the mapping.
        Creates new value using the ``default_factory``

        Example:

            >>> from ubii.framework.topics import TopicStore
            >>> class Topic:
            ...     def __init__(self, pattern):
            ...             self.pattern = pattern
            ...
            >>> store = TopicStore(default_factory=Topic)
            >>> topic = store['topic/glob/pattern']
            >>> assert topic.pattern == 'topic/glob/pattern'

        Args:
            key: glob pattern

        """
        self.data[key] = self._default_factory(key)

    def __getitem__(self, key: str) -> Topic_co:
        if key not in self.data:
            self.create_topic(key)
        assert key in self.data
        return self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.data)

    def __contains__(self, item):
        return item in self.data

    def __delitem__(self, key):
        if key not in self.data:
            raise KeyError(f"Can't delete item with key {key}")

        del self.data[key]


class StreamSplitRoutine(util.CoroutineWrapper[typing.Any, typing.Any, None]):
    """
    A StreamSplitRoutine splits :class:`TopicDataRecords <ubii.proto.TopicDataRecord>`
    from a :class:`~ubii.proto.TopicData` stream to the buffers of topics from a :class:`TopicStore` container

    Warning:

        This Coroutine only works with Topics with :class:`~ubii.proto.TopicDataRecord` buffers.
        For topics with different buffer type, use an adjusted :class:`StreamSplitRoutine`.
    """

    def __init__(self, *,
                 stream: typing.AsyncIterator[ubii.proto.TopicData],
                 container: TopicStore[Topic[ubii.proto.TopicDataRecord, typing.Any]],
                 logger: logging.Logger | None = None):
        """
        Set the static attributes for the coroutine, the :class:`~ubii.proto.TopicData` source and
        :class:`~ubii.proto.TopicDataRecord` container

        Args:
            stream: source for data
            container: sink for data
            logger: optional logger for processed data
        """

        self._stream: typing.AsyncIterator[ubii.proto.TopicData] = stream
        self._container = container
        self._logger = logger or logging.getLogger(__name__)
        super().__init__(coroutine=self.split_to_topics())

    async def make_record(self) -> typing.AsyncIterator[ubii.proto.TopicDataRecord]:
        """
        Creates iterator for :class:`~ubii.proto.TopicDataRecord` messages from the
        :class:`~ubii.proto.TopicData` iterator passed as ``stream`` during initialization,
        by unpacking possible :attr:`~ubii.proto.TopicData.topic_data_record_list`
        fields

        Returns:
            Iterator over individual records
        """
        async for data in self._stream:
            if data.topic_data_record_list:
                for record in data.topic_data_record_list.elements:
                    yield record
            elif data.topic_data_record:
                yield data.topic_data_record
            else:
                raise data.error

    async def split_to_topics(self) -> None:
        """
        Uses :meth:`.make_record` to create a stream of records, then iterates over them and

            *   creates the topic inside the :class:`TopicStore` passed as ``container`` during initialization
                if no topics matching the glob pattern of received
                :class:`TopicDataRecords <ubii.proto.TopicDataRecord>` are found
            *   sets the topic's :attr:`~Topic.buffer` to the record
        """
        async for record in self.make_record():
            topics = self._container.match_name(record.topic)
            self._logger.debug(f"Record Topic: {record.topic} -> matching: {','.join(map(str, topics))}")
            if not topics:
                topics = (self._container[record.topic],)
                log.warning(f"No topics found for record with topic {record.topic}")

            await asyncio.gather(*[topic.buffer.set(record) for topic in topics or ()])


class OnSubscribeCallback(Protocol):
    async def __call__(self, client_id: str, *topic_patterns: str, as_regex: bool = ..., unsubscribe: bool = ...):
        """
        Callbacks for the :class:OnSubscribersChange` objects need to have this signature

        Args:
            client_id: id of client node
            *topic_patterns: wildcard patterns or simple strings
            as_regex: patterns need to be handled differently from simple strings
            unsubscribe: is this a subscription or a subscription cancellation?
        """


class OnSubscribersChange:
    """
    This callable is a proxy for its :attr:`.callback` (see :class:`OnSubscribeCallback`).

    Attributes:
        as_regex (bool): static argument for eventual call to :attr:`.callback`
        client_id (str): static argument for eventual call to :attr:`.callback`
        event (asyncio.Event): application code can wait for this event -- is set whenever this is called
        callback (OnSubscribeCallback): actual callback
    """

    def __init__(self,
                 client_id: str,
                 as_regex: bool,
                 callback: OnSubscribeCallback):
        self.as_regex = as_regex
        self.client_id = client_id
        self.event = asyncio.Event()
        self.callback = callback

    def __call__(self, topic: Topic, change: typing.Tuple[int, int]):
        """
        Called when subscriber counts change in a topic.

        Executes the :attr:`.callback` when the subscriber count increases to 1 or decreases to 0 -- with args
            *   client_id = :attr:`.client_id`
            *   topic_patterns = (attr:`topic.pattern <Topic.pattern>`)
            *   as_regex = :attr:`.as_regex`
            *   unsubscribe = True if subscriber count decreased to 0, False if it was increased to 1

        Args:
            topic: typically the topic which owns this callable
            change: old and new subscriber count

        """
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
