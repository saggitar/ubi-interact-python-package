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

import itertools

import abc
import asyncio
import contextlib
import fnmatch
import functools
import logging
import proto
import re
import typing
import warnings
import weakref

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
                 buffer: typing.AsyncIterator[T_Buffer],
                 callback: Consumer[T_Buffer]):
        """
        Args:
            buffer: Iterator producing buffer value
            callback: a callable consuming the buffer values
        """
        self.__name__ = callback.__name__ if hasattr(callback, '__name__') else repr(callback)  # type: ignore
        self.topic = buffer
        self.callback = util.make_async(callback)
        super().__init__(coroutine=self._run())

    async def _run(self):
        async for value in self.topic:
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
        value = await self.buffer.get()
        assert value is not None
        return value

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
        self.on_subscribers_change: OnSubscribersChange | None = None
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

        self._callback_tokens: typing.Dict[int, typing.Tuple[T_Token, ...]] = {}

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

    @cached_property
    def registered_callbacks(self) -> typing.Container[typing.Callable]:
        """
        Use this property to check if a callback is registered in this topic

        Example:
            >>> import itertools
            >>> topic = Topic('foo', token_factory=itertools.count().__next__)
            >>> print in topic.registered_callbacks
            False
            >>> token = topic.register_callback(print)
            >>> print in topic.registered_callbacks
            True
        """

        class id_container:
            def __init__(self, mapping: typing.Mapping[T_Token, typing.Any]):
                self._mapping = mapping

            def __contains__(self, item):
                return id(item) in self._mapping

        return id_container(self._callback_tokens)

    def get_token(self, callback: typing.Callable) -> typing.Tuple[T_Token]:
        """
        If the callback is registered in this topic, get the registration token[s]

        Args:
            callback: some callback that could have been registered previously

        Returns:
            tuple of all registration tokens for callable or None if callback was not registered
        """
        return self._callback_tokens.get(id(callback), None)

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
            TopicCoroutine(buffer=self, callback=callback)
        )

        # callbacks might not be hashable, so we use their id to attach the information
        existing = self._callback_tokens.setdefault(id(callback), ())
        self._callback_tokens[id(callback)] = existing + (token,)
        weakref.finalize(callback, lambda key: self._callback_tokens.pop(key, None), id(callback))

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

        stop = asyncio.create_task(self.task_nursery.stop_task(task))
        _, pending = await asyncio.wait((stop,), timeout=timeout)
        return not pending


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
        self._buffer_value: ubii.proto.TopicDataRecord | None = None

    def _set_buffer(self, value):
        self._buffer_value = value

    def _get_buffer(self):
        return self._buffer_value

    @cached_property
    def buffer(self) -> util.accessor[ubii.proto.TopicDataRecord]:
        """
        You can use the :meth:`~codestare.async_utils.descriptor.accessor.get` and
        :meth:`~codestare.async_utils.descriptor.accessor.set` methods to synchronize access to the internal
        :class:`ubii.proto.TopicDataRecord` value.

        See Also:
           :class:`~codestare.async_utils.descriptor.accessor` -- details about access to shared resource

        """
        return util.accessor(funcs=(self._get_buffer, self._set_buffer), name='buffer')


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

    class helpers:
        """
        Some decorators to decorate the :meth:`~TopicStore.on_create` method that can be useful
        """

        class on_create_register_callback:
            """
            This decorator can simply be applied once, if you later want to add additional callbacks
            just add them to the :attr:`.callbacks` of the registered :class:`on_create_register_callback`
            """

            def __init__(self, *callbacks):
                self.callbacks = callbacks

            def __call__(self, on_create: util.hook):
                @functools.wraps(on_create)
                def decorated(store: TopicStore, key: str):
                    on_create(store, key)
                    for cb in self.callbacks:
                        store[key].register_callback(cb)

                return decorated


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
        self.as_regex: bool = as_regex
        self.client_id: str = client_id
        self.event: asyncio.Event = asyncio.Event()
        self.callback: OnSubscribeCallback = callback

    def __call__(self, topic: Topic, change: typing.Tuple[int, int]) -> asyncio.Task | None:
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
        task = None

        if new == 0 and old > 0:
            task = topic.task_nursery.create_task(
                self.callback(self.client_id, topic.pattern, as_regex=self.as_regex, unsubscribe=True)
            )

        if new == 1 and old < 1:
            task = topic.task_nursery.create_task(
                self.callback(self.client_id, topic.pattern, as_regex=self.as_regex, unsubscribe=False)
            )

        if task:
            task.add_done_callback(lambda _: self.event.set())

        return task


class MetaMuxRecord(ubii.proto.TopicDataRecord, metaclass=ubii.proto.ProtoMeta):
    """
    TopicDataRecords need to get additional meta information when they are produced by a Muxer.
    This meta information is not part of the proto schema. This mechanism is similar to the JS way
    muxers ands demuxers are implemented, to keep the processing code comparable.
    """

    __record_meta__: typing.MutableMapping[int, typing.MutableMapping] = {}

    def __init__(self, mapping: typing.Dict | proto.Message = None, *, ignore_unknown_fields: bool = False, **kwargs):
        """

        Args:
            mapping: A dictionary or message to be
                used to determine the values for this message. If it is a dictionary, keys that are not part
                of the proto schema will be added to the metadata instead
            ignore_unknown_fields: If True, do not raise errors for
                unknown fields. Only applied if `mapping` is a mapping type or there
                are keyword parameters.
            **kwargs: Keys and values corresponding to the fields of the message.
                Any keyword arguments that are passed on initialisation, that are not part of
                the :class:`~ubii.proto.TopicDataRecord` schema, will be simply inserted into the metadata mapping.
        """
        field_names = list(ubii.proto.TopicDataRecord.pb().DESCRIPTOR.fields_by_name)

        # let's help super().__init__  a bit by handling unusual behaviours
        if isinstance(mapping, ubii.proto.TopicDataRecord):
            mapping = ubii.proto.TopicDataRecord.pb(mapping)
        elif isinstance(mapping, typing.Mapping):
            kwargs.update(**{k: v for k, v in mapping.items() if k not in field_names})
            mapping = {k: v for k, v in mapping.items() if k in field_names}

        metadata = {k: v for k, v in kwargs.items() if k not in field_names}
        kwargs = {k: v for k, v in kwargs.items() if k in field_names}

        super().__init__(mapping=mapping, ignore_unknown_fields=ignore_unknown_fields, **kwargs)
        self.metadata(**metadata)

    def metadata(self: MetaMuxRecord, **kwargs) -> typing.Optional[typing.MutableMapping]:
        """
        :class:`~ubii.proto.TopicDataRecord` overwrites attribute access, so we can't write any
        attributes that are not part of the protobuf specification.

        This method allows saving metadata for a record in a specified way, without altering
        the proto schema.

        Args:
            **kwargs: If you specify keyword arguments, they will be written to the metadata mapping

        Returns:
            if metadata has been associated with the record, returns the metadata mapping, otherwise returns None
        """
        if kwargs:
            if id(self) not in self.__record_meta__:
                # add finalizer to remove object id when record is garbage collected
                weakref.finalize(self, lambda key: self.__record_meta__.pop(key, None), id(self))
            mapping = self.__record_meta__.setdefault(id(self), {})
            mapping.update(kwargs)

        return self.__record_meta__.get(id(self), None)

    @property
    def type(self) -> str | None:
        """
        Returns attribute name of the attribute that is set in the `type` oneof group for convenience
        """
        return type(self).pb(self).WhichOneof('type')


class TopicMuxer(ubii.proto.TopicMux, metaclass=util.ProtoRegistry):
    """
    A :class:`TopicMuxer` defines a special :attr:`.records` attribute, to handle topic data records. The muxer
    will identify metadata for the records, and associate it with them.
    """
    __unique_key_attr__ = 'id'

    def __init__(self, mapping=None, **kwargs):
        if isinstance(mapping, ubii.proto.TopicMux):
            mapping = ubii.proto.TopicMux.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)

        if not self.id:
            raise ValueError(f"Can't create {type(self)} object without valid 'id'")

        self._records: typing.MutableMapping[typing.Any, TopicMuxer.MuxedRecord] = {}

    def _compute_identity(self, record):
        identity_matches = re.findall(self.identity_match_pattern, record.topic)
        if len(identity_matches) > 0:
            if len(identity_matches) != 1:
                warnings.warn(f"{len(identity_matches)} identity matches found for topic {record.topic}, using first."
                              f" Consider refining the 'identity_match_pattern' regular expression "
                              f"{self.identity_match_pattern!r}")

            return identity_matches[0]

    def _get_records(self) -> 'typing.List[MetaMuxRecord]':
        return list(self._records.values())

    def _identify_records(self, records: typing.List[ubii.proto.TopicDataRecord]):
        for record in records:
            assert isinstance(record, ubii.proto.TopicDataRecord)
            assert 'topic' in record, "Invalid record, no topic set!"

            metarecord = MetaMuxRecord(mapping=record, identity=self._compute_identity(record))
            if metarecord.type and self.data_type != metarecord.type:
                continue

            # we are not updating any records client code might have saved. If you want to get up to date records,
            # use the records condition property
            self._records[record.topic] = metarecord

    records = util.condition_property(fget=_get_records, fset=_identify_records)
    """
    Use this :class:`util.condition_property` to read and write records handled by the
    muxer (which will convert them to :class:`MetaMuxRecord` objects
    that carry additional meta information)

    Example:
        *The example uses an async interpreter* ::
        
            >>> import asyncio
            >>> from ubii.framework.topics import TopicMuxer
            >>> import ubii.proto
            >>> muxer = TopicMuxer(
            ...     id="fake-id",
            ...     data_type='int32',
            ...     topic_selector='/topic/*',
            ...     identity_match_pattern='(?:/topic/([0-9a-z-]+))'
            ... )
            >>> records = [ubii.proto.TopicDataRecord(topic=f"/topic/{num}", int32=num) for num in range(5)]
            >>> records
            [topic: "/topic/0"
            int32: 0
            , topic: "/topic/1"
            int32: 1
            , topic: "/topic/2"
            int32: 2
            , topic: "/topic/3"
            int32: 3
            , topic: "/topic/4"
            int32: 4
            ]
            >>> await muxer.records.set(records)
            >>> muxed = await muxer.records.get(predicate=lambda _ : True)
            >>> muxed[0].metadata()
            {'identity': '0'}
            
    """


class TopicDemuxer(ubii.proto.TopicDemux, metaclass=util.ProtoRegistry):
    """
    A Demuxer converts :class:`MetaMuxRecord` objects to regular records, by setting their attributes
    (currently only the topic) according to their associated metadata and it's own attributes.
    """
    DEFAULT_REGEX_OUTPUT_PARAM = r'{{#([0-9]+)}}'
    __unique_key_attr__ = 'id'

    def __init__(self, mapping=None, param_regex=DEFAULT_REGEX_OUTPUT_PARAM, **kwargs):
        if isinstance(mapping, ubii.proto.TopicDemux):
            mapping = ubii.proto.TopicDemux.pb(mapping)

        super().__init__(mapping=mapping, **kwargs)

        if not self.id:
            raise ValueError(f"Can't create {type(self)} object without valid 'id'")

        self._param_regex = re.compile(param_regex)

    @staticmethod
    def _replace_with_param(params: typing.Tuple | None, match: re.Match):
        # fallback = whole match, no replacements

        fallback = match.group()
        if not len(match.groups()) == 1:
            warnings.warn(f"{len(match.groups())} groups in match, 1 is required")
            return fallback

        try:
            num_param = int(match.group(1))
        except TypeError:
            warnings.warn(f"Can't convert first match group {match.group(1)} to integer")
            # fallback, return whole match, don't replace
            return fallback

        if params is None:
            warnings.warn(f"No parameters specified")
            return fallback

        try:
            param = params[num_param]
        except IndexError:
            warnings.warn(f"Param {num_param} from parameters {params} can't be accessed")
            return fallback

        return param

    def _convert(self, meta_record: MetaMuxRecord | typing.Dict) -> ubii.proto.TopicDataRecord | None:
        if not isinstance(meta_record, MetaMuxRecord):
            meta_record = MetaMuxRecord(mapping=meta_record)

        if meta_record.type and self.data_type != meta_record.type:
            return

        topic = self._param_regex.sub(
            functools.partial(self._replace_with_param, meta_record.metadata().get('output_params')),
            self.output_topic_format
        )

        meta_record.topic = topic or meta_record.topic
        return meta_record

    def convert_record_objects(self, records: typing.List) -> ubii.proto.TopicDataRecordList:
        """
        The Demuxer converts the records to actual TopicDataRecords with the right topic

        Args:
            records: records with metadata, likely created as outputs of a processing module

        Returns:
            elements contain all records that where successfully converted

        Example:
            *The example uses an async interpreter* ::

                >>> import asyncio
                >>> from ubii.framework.topics import TopicDemuxer
                >>> import ubii.proto
                >>> records = [{'int32': num, 'output_params': (str(num),)} for num in range(5)]
                >>> demuxer = TopicDemuxer(
                ...     id='fake-id',
                ...     data_type='int32',
                ...     output_topic_format='/topic/{{#0}}'
                ... )
                >>> demuxer.convert_record_objects(records)
                elements {
                  topic: "/topic/0"
                  int32: 0
                }
                elements {
                  topic: "/topic/1"
                  int32: 1
                }
                elements {
                  topic: "/topic/2"
                  int32: 2
                }
                elements {
                  topic: "/topic/3"
                  int32: 3
                }
                elements {
                  topic: "/topic/4"
                  int32: 4
                }
        """

        return ubii.proto.TopicDataRecordList(elements=[r for r in map(self._convert, records) if r is not None])
