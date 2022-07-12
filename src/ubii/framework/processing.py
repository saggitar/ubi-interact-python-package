from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import contextlib
import dataclasses
import enum
import functools
import logging
import types
import typing

import ubii.proto

from . import (
    protocol,
    util,
    topics
)
from .util.typing import AsyncGetter, AsyncSetter

__protobuf__ = ubii.proto.__protobuf__

log = logging.Logger(__name__)

__json_name_to_field_name__ = {field.json_name: field.name for field in
                               ubii.proto.TopicDataRecord.pb().DESCRIPTOR.fields}
"""
Used in :meth:`.ProcessingProtocol.helpers.fix_io_fmt` to compute the field name of the 
:attr:`ubii.proto.TopicDataRecord.type` one-of group from the message type given by a 
:attr:`ubii.proto.ModuleIO.message_format` field. 
"""


def fix_io_fmt(message_format: str) -> str:
    """
    Computes the field name of the :attr:`~ubii.proto.TopicDataRecord.type` oneof corresponding to the
    type in a :attr:`~ubii.proto.ModuleIO.message_format` field (of the form ``ubii.{proto_package}.{type}``
    as defined in the .proto file, not the python package!)

    Example:

        The :attr:`~ProcessingRoutine.inputs` of a routine contain a
        :class:`ubii.proto.ModuleIO` message with :attr:`~ubii.proto.ModuleIO.message_format`
        ``ubii.dataStructure.Matrix4x4``. The name of the corresponding field in a
        :class:`ubii.proto.TopicDataRecord` is ``matrix4x4``.
        This method performs this conversion.

    Args:
        message_format: format string for message type

    Returns:
        name of corresponding field in a :class:`ubii.proto.TopicDataRecord` message

    """
    if not message_format.startswith('ubii.'):
        return message_format

    ubii_name = ubii.proto.util.get_import_name(message_format)
    field_name = __json_name_to_field_name__.get(
        f'{ubii_name.type[0].lower() + ubii_name.type[1:]}'
    )
    assert field_name, f"fixing field name for {message_format} failed."
    return field_name


MAX_WORKERS = 8


def default_perf_calc(scheduler: 'Scheduler') -> float:
    """
    Returns:
        performace of scheduler, calculated by computing relative error of average past execution times to
        :attr:`~Scheduler.delay` i.e. for an average of past execution times
        :math:`\\overline{t} = avg(\\text{scheduler.delta_times})` return :math:`1` if
        :math:`\\overline{t} < \\text{scheduler.delay}` otherwise calculate the relative deviation
        :math:`\\Delta t = \\frac{\\overline{t} - \\text{scheduler.delay}}{\\text{scheduler.delay}}` and return
        :math:`1 - \\Delta t` (negative values if :math:`\\Delta t > 1` allowed)

    """
    values = scheduler.delta_times
    if not values:
        return 1.

    avg = sum(values) / len(values)
    if avg < scheduler.delay:
        return 1.  # 100 % performance
    else:
        return 1. - (avg - scheduler.delay) / scheduler.delay  # 1 - rel error


def check_datatype(io: ubii.proto.ModuleIO, record: ubii.proto.TopicDataRecord):
    """
    Check if ModuleIO datatype matches record 'type' oneof name

    Args:
        io: a IO definition
        record: a record

    Returns:
        True if the right field is set, false otherwise

    See Also:
        :func:`fix_io_fmt` -- helper to translate message specification to oneof field name
    """
    return bool(record) and fix_io_fmt(io.message_format) == ubii.proto.TopicDataRecord.pb(record).WhichOneof('type')


class Scheduler(util.CoroutineWrapper):
    """
    A Scheduler is a wrapper around the coroutine created by its :meth:`get_trigger_loop` method.
    A scheduler -- depending on the used :attr:`.mode` -- waits for a certain delay and / or until
    any or all of its inputs are available and then schedules its callback.
    """

    class LoopTime:
        """
        Utility class to calculate execution times
        """

        def __init__(self, loop: asyncio.AbstractEventLoop):
            self.value = None
            self.loop = loop

        def record(self) -> None:
            """
            Records current loop time
            """
            self.value = self.loop.time()

        def delta(self) -> float:
            """
            Evaluate delta between current loop time and last recorded loop time.
            Returns 0 if no time has been recorded yet
            """
            return self.loop.time() - self.value if self.value else 0.

    def __init__(self,
                 callback: typing.Callable[[], None],
                 inputs: typing.Iterable[AsyncGetter],
                 mode: ubii.proto.ProcessingMode,
                 *,
                 perf_metric=default_perf_calc):
        """
        The mode and inputs determine when the callback is executed.

        Args:
            callback: Schedule execution of this callable when conditions are right
            inputs: depending on the :class:`~ubii.proto.ProcessingMode` inputs are part of the condition --
                or simply retrieved -- for execution of the callback. For each callable passed in ``inputs`` the
                returned :class:`~typing.Awaitable` will either be in :attr:`.done` or :attr:`.pending` when
                the callback is scheduled
            mode: used :class:`~ubii.proto.ProcessingMode`
            perf_metric: callable to evaluate scheduler overhead / performance

        See Also:
            :attr:`ubii.proto.ProcessingMode.mode` -- details on processing modes
        """
        self._loop = asyncio.get_running_loop()
        self._perf_metric = perf_metric.__get__(self, type(self))

        self.inputs: typing.Iterable[AsyncGetter] = inputs
        """
        callables to create awaitables for possibly needed inputs
        """
        self.delta_times: typing.Deque = collections.deque(maxlen=30)
        """
        keep track of times between callback schedules for performance evaluation
        """
        self.done: typing.List[typing.Awaitable] = []
        """
        contains awaitables created from ``inputs`` argument that finished when callback needs to be scheduled
        """
        self.mode: 'ubii.proto.ProcessingMode' = mode
        """
        used mode, determines which conditions need to be matched to schedule the callback
        """
        self.callback: typing.Callable[[], None] = callback
        """
        callback to be scheduled
        """
        self.executor: 'concurrent.futures.ThreadPoolExecutor' = concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        )
        """
        callbacks are possibly non-async callables, will be scheduled in this executor using 
        :meth:`asyncio.loop.run_in_executor`
        """
        self.task_clean_frequency = 10
        """
        After every n callback schedules, cancel old input awaitables
        """
        self.loop_time: Scheduler.LoopTime = self.LoopTime(self._loop)
        """
        Helps to calculate loop times for execution scheduling
        """

        self._stop_during_next_iteration = False

        self._old_input_tasks = []
        self._scheduling_count = 0

        super().__init__(coroutine=self._trigger_loop())

    @property
    def delay(self) -> float:
        """
        Delay in seconds to schedule callback, depending on :attr:`.mode`

            *   `frequency` -- simply use :attr:`~ubii.proto.ProcessingMode.Frequency.hertz`
            *   `trigger_on_input` -- use :attr:`~ubii.proto.ProcessingMode.TriggerOnInput.min_delay_ms`

        Raises:
            NotImplementedError: when the mode is ``lockstep``
        """
        if self.mode.frequency:
            return 1.0 / self.mode.frequency.hertz
        if self.mode.trigger_on_input:
            return self.mode.trigger_on_input.min_delay_ms / 1000
        if self.mode.lockstep:
            raise NotImplementedError(f"Not implemented yet")

    @property
    def performance_rating(self) -> float:
        """
        Calculated using callable passed as ``perf_metric`` during initialization

        Raises:
            NotImplementedError: when the mode is ``lockstep``
        """
        if self.mode.lockstep:
            raise NotImplementedError(f"Not applicable")

        return self._perf_metric()

    def halt(self) -> None:
        """
        Call this method to stop the internal scheduler loop after the next iteration, which will finish the
        wrapped coroutine
        """
        self._stop_during_next_iteration = True

    def _wait(self):
        """
        Create input awaitables from :attr:`.inputs`, start possible coroutines as tasks

        Returns:
            awaitable that waits for inputs to become ready, depending on :attr:`.mode`
        """
        if self.mode.frequency:
            timeout = self.delay - self.loop_time.delta()
            timeout = timeout if timeout > 0 else 0
        else:
            timeout = None

        awts = [
            self._loop.create_task(awt) if asyncio.iscoroutine(awt) else awt
            for awt
            in [get() for get in self.inputs]
        ]

        return_when = (
            asyncio.ALL_COMPLETED
            if self.mode.trigger_on_input and self.mode.trigger_on_input.all_inputs_need_update else
            asyncio.FIRST_COMPLETED
        )
        return asyncio.wait(awts, timeout=timeout, return_when=return_when)

    async def _trigger_loop(self):
        """
        This method is used to generate the coroutine that is wrapped internally.
        There is no need to call this method manually, just `await` the scheduler.

        The created coroutine runs an internal loop that can be stopped using :meth:`.halt`, and schedules
        the :attr:`.callback` in the currently running `event loop` with the :attr:`.executor` whenever the
        :attr:`.inputs` are `ready` (this means that either `one` or `all` of the input awaitables finished,
        depending on the :attr:`.mode`) and / or the :attr:`.delay` has passed.
        """
        self.loop_time.record()

        with self.executor as pool:
            while not self._stop_during_next_iteration:

                self.done, self.pending = await self._wait()
                since_last_recorded_time = self.loop_time.delta()
                remaining = self.delay - since_last_recorded_time

                if remaining > 0:
                    await asyncio.sleep(remaining)
                    self.delta_times.append(self.loop_time.delta())
                else:
                    self.delta_times.append(since_last_recorded_time)

                await self._loop.run_in_executor(pool, self.callback)
                self.loop_time.record()

                self._scheduling_count += 1
                self._old_input_tasks.extend(self.pending)
                if self._scheduling_count % self.task_clean_frequency == 0:
                    self._scheduling_count = 0
                    await self._clean_old_inputs()

            await self._clean_old_inputs()

    async def _clean_old_inputs(self):
        with contextlib.suppress(asyncio.CancelledError):
            for awaitable in self._old_input_tasks:
                awaitable.cancel()
                await awaitable


@util.dunder.repr('id', 'name', 'status')
class ProcessingRoutine(ubii.proto.ProcessingModule, metaclass=util.ProtoRegistry):
    """
    This adds a :class:`ProcessingProtocol` providing processing behaviour to a
    :class:`~ubii.proto.ProcessingModule` representation.
    Refer to the documentation of the :class:`ProcessingProtocol` for
    information related to processing behaviour.
    """

    __unique_key_attr__ = 'name'
    """
    See documentation of :class:`ubii.framework.util.ProtoRegistry` for more information
    """

    class rules:
        """
        Rules to validate the protobuf message
        """

        @staticmethod
        def validate_language(pm: ProcessingRoutine):
            """
            Only python modules can be run by the python client node.
            Sets language to :attr:`~ubii.proto.ProcessingModule.Language.PY` if language is not set.

            Args:
                pm: needs validation

            Raises:
                ValueError: if :attr:`~ubii.proto.ProcessingModule.language` is already set,
                    but not to :attr:`~ubii.proto.ProcessingModule.Language.PY`
            """
            if pm.language and pm.language != pm.Language.PY:
                raise ValueError(f"{pm} can only run Python processing modules. language {pm.language!r} specified.")
            pm.language = pm.Language.PY

        @staticmethod
        def validate_id(pm: ProcessingRoutine):
            """
            Check if :attr:`~ubii.proto.ProcessingModule.id` is present

            Args:
                pm: needs validation

            Raises:
                ValueError: if :attr:`~ubii.proto.ProcessingModule.id` is not set
            """
            if pm.id is None:
                raise ValueError(f"id of {pm} is not set")

    def __init__(self, mapping=None, eval_strings=False, **kwargs):
        """
        Args:
            mapping (Union[dict, Message]): A dictionary or message to be
                used to determine the values for this message.
            eval_strings (bool): If this flag is set or the framework is used in :func:`~ubii.framework.logging.debug`
                mode, fields of the protobuf message that have names ending with ``_stringified`` (e.g.
                :attr:`~ubii.proto.ProcessingModule.on_processing_stringified`) will be evaluated.
                Creating a :class:`~ProcessingRoutine` whose
                :attr:`~ubii.proto.ProcessingModule.language` is not :attr:`~ubii.proto.ProcessingModule.Language.PY`
                is not possible, see :func:`.validate`

            kwargs (dict): Keys and values corresponding to the fields of the
                message and other arguments passed to :class:`ubii.proto.ProcessingModule`
        """

        # we allow initialisation from ub.ProcessingModule Wrappers
        if isinstance(mapping, ubii.proto.ProcessingModule):
            mapping = ubii.proto.ProcessingModule.pb(mapping)

        self._protocol = ProcessingProtocol(pm=self)
        self._change_specs = asyncio.Condition()

        super().__init__(mapping=mapping, **kwargs)

        # eval stringified
        if eval_strings or util.debug() and self.language == self.Language.PY:
            self._eval_string_funcs()

        self.validate()

        self._local_output_topics = topics.TopicStore(
            default_factory=functools.partial(topics.BasicTopic, task_nursery=self._protocol.task_nursery)
        )

        self._input_source_getter = None
        self._output_destination_getter = None

    def _eval_string_funcs(self):
        suffix = '_stringified'
        string_fields = [
            name for name in ubii.proto.ProcessingModule.pb(self).DESCRIPTOR.fields_by_name
            if name.endswith(suffix) and getattr(self, name)
        ]
        definitions = map(functools.partial(getattr, self), string_fields)
        for name, source in zip(string_fields, definitions):
            created_name = name[:-len(suffix)]
            ns = {}
            exec(source, {}, ns)
            func = ns.get(created_name)
            if not func or not callable(func):
                raise ValueError(f"Evaluating {name} did not create a function named {created_name}")

            setattr(type(self), created_name, func)

    @property
    def local_output_topics(self) -> 'topics.TopicStore':
        """
        Container to interact with the output topics of the module (e.g. register callbacks on the topics).
        """
        return self._local_output_topics

    @property
    def change_specs(self) -> 'asyncio.Condition':
        """
        Coordinate access to the protobuf specs of this module. Whenever part of the wrapped protobuf message changes,
        this should be done while acquiring this condition.

        When the :attr:`~ubii.proto.ProcessingModule.status` or input / output mappings change as part of
        :meth:`running <.start>` this protocol, coroutines waiting for this condition are notified automatically.

        Example: ::

            def change_output_mapping(pm: ProcessingRoutine, io: ubii.proto.ModuleIO):
                async with pm.change_specs:
                    pm.outputs = [io]
                    pm.change_specs.notify_all()

        """
        return self._change_specs

    def input_getter(
            self, io: ubii.proto.ModuleIO
    ) -> AsyncGetter[ubii.proto.TopicDataRecord | ubii.proto.TopicDataRecordList] | None:
        if not self._input_source_getter:
            return None

        return self._input_source_getter(io)

    def output_setter(self, io: ubii.proto.ModuleIO) -> AsyncSetter | None:
        if not self._output_destination_getter:
            return None

        return self._output_destination_getter(io)

    async def apply_io_mapping(self,
                               io_mapping: ubii.proto.IOMapping,
                               remote_topic_map: typing.Mapping[str, topics.Topic]):
        """
        Extract relevant information from a :class:`ubii.proto.IOMapping` message to initialize the
        :attr:`.get_output_topic` and :attr:`.get_input_topic` callables.

        Notifies awaitables waiting on :attr:`.change_specs` unless the mapping was empty.

        Args:
            io_mapping: Mapping that should be applied to this module
            remote_topic_map: While output topics will be managed by :attr:`.local_output_topics`, you need to provide
                a mapping for input topics to look up the topic source of the
                :attr:`ubii.proto.IOMapping.input_mappings`
        """
        get_input_mapping = {mapping.input_name: mapping for mapping in io_mapping.input_mappings}.get
        get_output_mapping = {mapping.output_name: mapping for mapping in io_mapping.output_mappings}.get

        def getter(topic_map: typing.Mapping[..., topics.Topic],
                   io: ubii.proto.ModuleIO) -> typing.Callable[[], typing.Awaitable]:
            """
            Depending on the :class:`~ubii.proto.InputMapping` for the IO specification,
            return a callable that returns an awaitable to get the next record in a topic,
            or the :attr:`~TopicMuxer.records` of a muxer

            Args:
                topic_map: This topic map will manage the topics that we access
                io: use this specification to look up the :class:`~ubii.proto.InputMapping` in the
                    :class:`~ubii.proto.IOMapping` passed to :meth:`apply_io_mapping`

            Returns:
                a callable returning an awaitable to await new topic data
            """
            mapping: ubii.proto.TopicInputMapping = get_input_mapping(io.internal_name)
            if mapping is None:
                raise ValueError(f"No mapping with input name {io.internal_name} found")

            if mapping.topic_mux:
                muxer = topics.TopicMuxer.registry.get(mapping.topic_mux.id)
                if not muxer:
                    muxer = topics.TopicMuxer(mapping=mapping.topic_mux)
                    topic_map[muxer.topic_selector].register_callback(lambda record: muxer.records.set([record]))

                muxer_getter = (
                    functools.partial(muxer.records.get, predicate=lambda: True)
                    if self.processing_mode.frequency else
                    muxer.records.get
                )
                return muxer_getter

            elif mapping.topic:
                return functools.partial(
                    topic_map[mapping.topic].buffer.get,
                    predicate=functools.partial(check_datatype, io),
                    wait_for_write=not self.processing_mode.frequency
                )
            else:
                raise ValueError(f"Invalid input mapping {mapping}")

        def setter(topic_map: typing.Mapping[..., topics.Topic],
                   io: ubii.proto.ModuleIO) -> typing.Callable[..., typing.Awaitable[None]]:

            """
            Depending on the :class:`~ubii.proto.OutputMapping` for the IO specification,
            return a callable that returns an awaitable to set the record in a topic (using a :class:`TopicDemuxer`
            is necessary)

            Args:
                topic_map: This topic map will manage the topics that we access
                io: use this specification to look up the :class:`~ubii.proto.OutputMapping` in the
                    :class:`~ubii.proto.IOMapping` passed to :meth:`apply_io_mapping`

            Returns:
                a callable returning an awaitable to set topic data
            """
            mapping: ubii.proto.TopicOutputMapping = get_output_mapping(io.internal_name)
            if mapping is None:
                raise ValueError(f"No mapping with input name {io.internal_name} found")

            async def set_value(record: ubii.proto.TopicDataRecord, default_topic=mapping.topic) -> None:
                """
                Set data for topic from the topic map, if the records datatype matches the datatype
                specified in the :class:`~ubii.proto.ModuleIO` specification

                Args:
                    record: a topic data record that should be written to a topic
                    default_topic: the default topic that should be used if the record has no topic set

                """
                assert isinstance(record, ubii.proto.TopicDataRecord)

                if not fix_io_fmt(io.message_format) in record:
                    log.warning(f"{record} does not have the {fix_io_fmt(io.message_format)} field set required"
                                f" by the ModuleIO data format, skipping")
                    return

                record.topic = record.topic or default_topic
                topic = topic_map[record.topic]

                # if the previous call creates a new topic with default callbacks, setting the record instantly
                # might not trigger the callback, so we wait until the topic buffer is ready
                # then write the data
                await topic.buffer.set(record, wait_for_read=True)

            if mapping.topic_demux:
                demuxer = topics.TopicDemuxer.registry.get(mapping.topic_demux.id)
                if not demuxer:
                    demuxer = topics.TopicDemuxer(mapping=mapping.topic_demux)

                async def set_value_demux(records: typing.List) -> None:
                    """
                    When demuxing, the :class:`TopicDemuxer` converts the record objects
                    to actual :class:`ubii.proto.TopicDataRecord` messages

                    Args:
                        records: objects that represent a record with additional meta information
                    """
                    for record in demuxer.convert_record_objects(records).elements:
                        await set_value(record)

                return set_value_demux
            elif mapping.topic:
                return set_value
            else:
                raise ValueError(f"Invalid output mapping {mapping}")

        # input is retrieved from remote topic map, output topics are created locally for each processing module
        self._input_source_getter = functools.partial(getter, remote_topic_map)
        self._output_destination_getter = functools.partial(setter, self.local_output_topics)

        if io_mapping.output_mappings or io_mapping.input_mappings:
            async with self.change_specs:
                self.change_specs.notify_all()

    @util.hook
    @util.document_decorator(util.hook)
    def on_created(self, context: types.SimpleNamespace) -> None:
        """
        Will be executed by the :class:`ProcessingProtocol` whenever
        :meth:`ProcessingProtocol.on_created` is called

        Args:
            context: Same context that is used by :meth:`ProcessingProtocol.on_created`

        """

    @util.hook
    @util.document_decorator(util.hook)
    def on_init(self, context: types.SimpleNamespace) -> None:
        """
        Will be executed by the :class:`ProcessingProtocol` whenever
        :meth:`ProcessingProtocol.on_init` is called

        Args:
            context: Same context that is used by :meth:`ProcessingProtocol.on_init`

        """

    @util.hook
    @util.document_decorator(util.hook)
    def on_processing(self, context: types.SimpleNamespace) -> None:
        """
        Will be executed by the :class:`ProcessingProtocol` whenever
        :meth:`ProcessingProtocol.on_processing` is called

        Args:
            context: Same context that is used by :meth:`ProcessingProtocol.on_processing`

        """

    @util.hook
    @util.document_decorator(util.hook)
    def on_halted(self, context: types.SimpleNamespace) -> None:
        """
        Will be executed by the :class:`ProcessingProtocol` whenever
        :meth:`ProcessingProtocol.on_halted` is called

        Args:
            context: Same context that is used by :meth:`ProcessingProtocol.on_halted`

        """

    @util.hook
    @util.document_decorator(util.hook)
    def on_destroyed(self, context: types.SimpleNamespace) -> None:
        """
        Will be executed by the :class:`ProcessingProtocol` whenever
        :meth:`ProcessingProtocol.on_destroyed` is called

        Args:
            context: Same context that is used by :meth:`ProcessingProtocol.on_destroyed`

        """

    def validate(self):
        """
        Run all rules in :attr:`.validation_rules`
        """

        for rule in self.validation_rules:
            rule(self)

    @classmethod
    @util.hook
    @util.document_decorator(util.hook)
    async def start(cls, pm: ProcessingRoutine) -> ProcessingRoutine:
        """
        Start the internal :class:`ProcessingProtocol` of the passed routine.

        Args:
            pm: instance that needs to be started

        Returns:
            routine passed as ``pm``
        """
        assert pm.name in cls.registry
        pm._protocol.start()
        return pm

    @classmethod
    @util.hook
    @util.document_decorator(util.hook)
    async def stop(cls, pm: ProcessingRoutine):
        """
        Stop the internal :class:`ProcessingProtocol` of the passed routine and
        remove the routine from the registry.

        Args:
            pm: instance that needs to be stopped

        Returns:
            routine passed as argument
        """
        assert cls.registry.pop(pm.name) == pm
        await pm._protocol.stop()
        return pm

    @classmethod
    @util.hook
    @util.document_decorator(util.hook)
    async def halt(cls, pm: ProcessingRoutine):
        """
        Halt the internal :class:`ProcessingProtocol` of the passed routine.

        Args:
            pm: instance that needs to be halted

        Returns:
            routine passed as argument
        """
        assert pm.name in cls.registry
        await pm._protocol.state.set(PM_STAT.HALTED)
        return pm

    def __str__(self):
        info = f"name={self.name!r}"
        return f"{self.__class__.__name__}({info})"

    validation_rules: 'typing.List[typing.Callable[[ProcessingRoutine], None]]' = [
        rules.validate_language,
        rules.validate_id,
    ]
    """
    Callables to validate the protobuf message used in :meth:`.validate`
    
    See Also:
        :class:`ProcessingRoutine.rules` -- details on rules used here
    """


class PM_STAT(enum.IntFlag):
    """
    Proxy for :class:`~ubii.proto.ProcessingModule.Status` but as :class:`enum.IntFlag`, to allow
    defining combinations of states.
    """
    INITIALIZED = enum.auto()
    CREATED = enum.auto()
    PROCESSING = enum.auto()
    HALTED = enum.auto()
    DESTROYED = enum.auto()


class ProcessingProtocol(protocol.AbstractProtocol[PM_STAT]):
    """
    This :class:`~ubii.framework.protocol.AbstractProtocol` implementation defines the Protocol used to run
    :class:`ProcessingRoutines <ProcessingRoutine>`.

    It defines valid :attr:`.state_changes` and callbacks,
    as well as the :attr:`.starting_state` and :attr:`.end_state`

    The :class:`.pm_proxy` methods are used to decorate the lifecycle callbacks
    :meth:`.on_created`, :meth:`.on_init`, :meth:`.on_processing`, :meth:`.on_halted`, :meth:`.on_destroyed`,
    to execute the proxied methods in the :class:`~ProcessingRoutine` which owns
    the :class:`ProcessingProtocol` and set the
    :class:`ProcessingRoutine.status`

    Attributes:
        task_nursery: inherited from :class:`ubii.framework.protocol.AbstractProtocol`
        state: inherited from :class:`ubii.framework.protocol.AbstractProtocol`
    """

    starting_state = PM_STAT.INITIALIZED
    """
    Before the protocol has the starting state, it's :attr:`.state` is `None`
    """
    end_state = PM_STAT.DESTROYED
    """
    If the protocol ends up in its end state, the coroutine `running` the protocol will finish
    """
    AnyState = PM_STAT.INITIALIZED | PM_STAT.CREATED | PM_STAT.PROCESSING | PM_STAT.HALTED | PM_STAT.DESTROYED
    """
    This is a combination of all possible states to allow easier transitions in :attr:`.state_changes`
    """

    class pm_proxy:
        """
        Define some decorators for callables like :meth:`~ProcessingProtocol.on_processing`, because
        all lifecycle callbacks need to set the :attr:`~ProcessingRoutine.status` of the corresponding
        processing routine, and call the corresponding callbacks (e.g. if a :meth:`ProcessingProtocol.on_processing` callback
        is called, it needs to call the :meth:`ProcessingRoutine.on_processing` callback of the
        :attr:`managed processing routine <ProcessingProtocol.pm>`)
        """
        _Status = typing.Union[PM_STAT, ubii.proto.ProcessingModule.Status]

        @classmethod
        def _get_decorator(cls, callable_: typing.Callable) -> typing.Callable:
            def decorator(func):
                @functools.wraps(func)
                async def _inner(instance: ProcessingProtocol, context):
                    result = callable_(instance.pm, context)
                    if asyncio.iscoroutine(result):
                        await result
                    return await func(instance, context)

                return _inner

            return decorator

        @classmethod
        def set_status_in_pm(cls, status: ubii.proto.ProcessingModule.Status) -> typing.Callable:
            """
            Callable decorated with returned decorator sets the status of the
            :class:`~ProcessingRoutine` instance to the passed status when called,
            and notifies awaitables waiting for :attr:`~ProcessingRoutine.change_specs`

            Args:
                status: which status will be set

            Returns:
                Decorator
            """

            async def set_status(pm: ProcessingRoutine, _):
                async with pm.change_specs:
                    pm.status = status
                    pm.change_specs.notify_all()

            return cls._get_decorator(set_status)

        @classmethod
        def callback_in_pm(cls, name: str) -> typing.Callable:
            """
            Callable decorated with returned decorator calls the method with specified name of the
            :class:`~ProcessingRoutine` instance.
            arguments of decorated callable are passed on.

            Args:
                name: e.g. ``'on_destroyed'``

            Returns:
                Decorator
            """

            def callback(pm: ProcessingRoutine, *args):
                _cb = getattr(pm, name)
                assert callable(_cb)
                return _cb(*args)

            return cls._get_decorator(callback)

    class helpers:
        """
        Used for pre- / post-processing for the context data passed between lifecycle callbacks
        """

        __no_output__ = object()

        @classmethod
        def write_scheduler_data_to_context(cls, scheduler: Scheduler, ctx: types.SimpleNamespace) -> None:
            """
            Before processing, the processing context needs to be
            enriched with the topic data that the :class:`Scheduler` retrieved as inputs

            Args:
                scheduler: The Scheduler scheduling the callback
                ctx: The context that will be passed to the callback
            """
            ctx.delta_time = scheduler.delta_times[-1] if scheduler.delta_times else None
            inputs = vars(ctx.inputs)

            inputs.update(**{
                result.meta: result.value
                for result in map(lambda task: task.result(), scheduler.done)
            })
            ctx.inputs = types.SimpleNamespace(**inputs)

        @classmethod
        def publish_outputs_to_topics(cls, pm: ProcessingRoutine, ctx: types.SimpleNamespace):
            """
            Looks up topics via :meth:`~ProcessingRoutine.get_output_topic`,
            starts a async task to set the buffer value of the topic to the computed value from the
            context outputs, which triggers publishing if the topics has been set up correctly,
            see e.g. :meth:`ubii.node.node_protocol.implement_processing`.

            Args:
                pm: look up output topics in this routines
                    :meth:`~ProcessingRoutine.local_output_topics`
                ctx: extract computed outputs from this context

            """
            get_output_io = {io.internal_name: io for io in pm.outputs}.get

            for name, value in vars(ctx.outputs).items():
                if value == ProcessingProtocol.helpers.__no_output__:
                    continue

                io = get_output_io(name)
                if not io:
                    log.warning(f"No output topic for argument {name!r} defined in {pm}. Defined output handler[s] "
                                f"{', '.join(map(lambda o: repr(o.internal_name), pm.outputs))}.")
                    continue

                ctx.nursery.create_task(
                    pm.output_setter(io)(value)
                )

    def __init__(self, pm: ProcessingRoutine):
        """
        The created :class:`ProcessingProtocol` implements the behaviour of a :class:`~ubii.proto.ProcessingModule`
        or rather a :class:`ProcessingRoutine` during its lifetime. Each :class:`ProcessingProtocol` belongs to
        one :class:`ProcessingRoutine`

        Args:
            pm: the :class:`ubii.proto.ProcessingModule` wrapper that owns this protocol
        """
        super().__init__()
        self.pm: ProcessingRoutine = pm
        """
        reference to the :class:`~ubii.proto.ProcessingModule` wrapper that owns this protocol 
        """
        self.created_tasks: typing.List[asyncio.Task] = []
        """
        All tasks created by this protocol, to be stopped / cancelled when necessary
        """

    @pm_proxy.callback_in_pm('on_init')
    @pm_proxy.set_status_in_pm(ubii.proto.ProcessingModule.Status.INITIALIZED)
    async def on_init(self, context: types.SimpleNamespace):
        """
        First lifecycle method called after :attr:`pm` has been initialized

        Calls :meth:`pm.on_init() <ProcessingRoutine.on_init>` and sets the
        :attr:`~ProcessingRoutine.status` to :attr:`ub.ProcessingModule.Status.INITIALIZED`.

        Creates the following attributes in the context:

        -   ``context.loop`` the asyncio loop running the processing
        -   ``context.nursery`` the entity responsible for starting async tasks (either the ``context.loop`` or a
            reference to :attr:`.task_nursery`
        -   ``context.trigger_processing`` asyncio Event used later, see :meth:`.on_created`
        -   ``context.scheduler`` a :class:`Scheduler` instance, responsible for
            scheduling processing calls. Scheduled callback when inputs are available will set
            ``context.trigger_processing``
        -   ``context.muxer`` a reference to the :class:`TopicMuxer` type, needed to extract information
            from the records

        Finally, sets the :attr:`~ProcessingProtocol.status` to
        :attr:`PM_STAT.CREATED`

        Args:
            context: namespace object to hold data
        """

        # use the Processing Protocol as a task nursery if possible ?
        context.loop = asyncio.get_running_loop()
        context.nursery = self.task_nursery or context.loop
        context.muxer = topics.TopicMuxer
        context.demuxer = topics.TopicDemuxer

        # callable to create input_mapping dict from module io iterable
        make_input_dict: util.make_dict[typing.Tuple[str, str], AsyncGetter] = util.make_dict(
            key=lambda io: io.internal_name,
            value=self.pm.input_getter,
            filter_none=True
        )

        # wait for applied input mapping
        async with self.pm.change_specs:
            inputs = await self.pm.change_specs.wait_for(functools.partial(make_input_dict, self.pm.inputs))

        invalid_inputs = [input_values for input_values in inputs if not all(input_values)]
        if invalid_inputs:
            fmt = "{{ name: {!r}, format: {!r} }}"
            raise ValueError(f"Got invalid input[s]: {', '.join(map(lambda inp: fmt.format(*inp), invalid_inputs))}")

        # wait for applied output publishing
        async with self.pm.change_specs:
            await self.pm.change_specs.wait_for(
                lambda: all(topic.callback_tasks for topic in self.pm.local_output_topics.values())
            )

        # start scheduler task
        context.trigger_processing = asyncio.Event()
        context.scheduler = Scheduler(
            callback=context.trigger_processing.set,
            inputs=[util.enrich(name, getter) for name, getter in inputs.items()],
            mode=self.pm.processing_mode
        )

        self.created_tasks += [context.nursery.create_task(context.scheduler)]

        await self.state.set(PM_STAT.CREATED)

    @pm_proxy.callback_in_pm('on_created')
    @pm_proxy.set_status_in_pm(ubii.proto.ProcessingModule.Status.CREATED)
    async def on_created(self, context):
        """
        Calls :meth:`pm.on_created() <ProcessingRoutine.on_created>` and sets the
        :attr:`~ProcessingRoutine.status` to :attr:`ub.ProcessingModule.Status.CREATED`.

        Prepares the context for further processing:
        -   creates inputs (`context.inputs`)
        -   Prepare output mapping (`context.outputs`)

        Waits for ``context.processing_trigger`` then extracts inputs from ``context.scheduler`` (using
        :meth:`.helpers.write_scheduler_data_to_context`) and changes
        the :attr:`~ProcessingProtocol.status` to
        :attr:`PM_STAT.PROCESSING`
        """
        log.info(f"created processing module {self.pm}")
        # create inputs
        context.inputs = types.SimpleNamespace(**dict.fromkeys(map(lambda io: io.internal_name, self.pm.inputs)))

        # create outputs (use dataclass to get better error reporting)
        fields = [
            (out.internal_name, typing.Any, dataclasses.field(default=self.helpers.__no_output__))
            for out in self.pm.outputs
        ]
        outputs = dataclasses.make_dataclass('outputs', fields, init=True)  # noqa
        context.outputs = outputs()

        # wait until processing is triggered and change state to processing
        await context.trigger_processing.wait()
        self.helpers.write_scheduler_data_to_context(context.scheduler, context)
        await self.state.set(PM_STAT.PROCESSING)

    @pm_proxy.callback_in_pm('on_processing')
    @pm_proxy.set_status_in_pm(ubii.proto.ProcessingModule.Status.PROCESSING)
    async def on_processing(self, context):
        """
        Calls :meth:`pm.on_processing() <ProcessingRoutine.on_processing>` and sets the
        :attr:`~ProcessingRoutine.status` to
        :attr:`ubii.proto.ProcessingModule.Status.PROCESSING`.

        -   publishes computed outputs (see :meth:`.helpers.publish_outputs_to_topics`)
        -   adjusts the ``context.scheduler`` callback (previously used to simply trigger processing) to

            -   extract data from context using :meth:`.helpers.write_scheduler_data_to_context`
            -   compute with :meth:`~ProcessingRoutine.on_processing` of owner
            -   publish outputs to topics using :meth:`.helpers.publish_outputs_to_topics`

            such that as long as the :attr:`~ProcessingProtocol.status` does not
            change data will be processed.

        Does not change the :attr:`~ProcessingProtocol.status` of the protocol,
        state change has to be triggered externally in this state.
        """

        self.helpers.publish_outputs_to_topics(self.pm, context)

        # we just change the scheduler callback to get inputs and publish outputs.
        context.scheduler.callback = util.function_chain(
            functools.partial(self.helpers.write_scheduler_data_to_context, context.scheduler, context),
            functools.partial(self.pm.on_processing, context),
            functools.partial(self.helpers.publish_outputs_to_topics, self.pm, context),
        )

    @pm_proxy.callback_in_pm('on_halted')
    @pm_proxy.set_status_in_pm(ubii.proto.ProcessingModule.Status.HALTED)
    async def on_halted(self, context):
        """
        Calls :meth:`pm.on_halted() <ProcessingRoutine.on_halted>` and sets the
        :attr:`~ProcessingRoutine.status` to :attr:`ub.ProcessingModule.Status.HALTED`.

        :meth:`Halts <Scheduler.halt>` the ``context.scheduler``

        Does not change the :attr:`~ProcessingProtocol.status` of the protocol,
        state change has to be triggered externally in this state.
        """
        context.scheduler.halt()

    @pm_proxy.callback_in_pm('on_destroyed')
    @pm_proxy.set_status_in_pm(ubii.proto.ProcessingModule.Status.DESTROYED)
    async def on_destroyed(self, context):
        """
        Calls :meth:`pm.on_destroyed() <ProcessingRoutine.on_destroyed>` and sets the
        :attr:`~ProcessingRoutine.status` to :attr:`ub.ProcessingModule.Status.DESTROYED`.

        Awaits cancellation of  all tasks that have been started by this protocol.
        """
        for task in self.created_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    state_changes = {
        (None, PM_STAT.INITIALIZED): on_init,
        (AnyState, PM_STAT.CREATED): on_created,
        (AnyState, PM_STAT.PROCESSING): on_processing,
        (AnyState, PM_STAT.HALTED): on_halted,
        (PM_STAT.HALTED, PM_STAT.DESTROYED): on_destroyed,
        (PM_STAT.DESTROYED, PM_STAT.INITIALIZED): on_init,
    }
    """
    Possible state changes and respective callbacks
    """
