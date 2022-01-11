from __future__ import annotations

import dataclasses
from collections import namedtuple

from contextlib import suppress

from warnings import warn

import types

import asyncio
import enum
import logging
import typing as t
from functools import wraps, partial, cached_property

import codestare.async_utils as util
import ubii.proto as ub
from . import protocol, topics
from ._util import ProtoRegistry, function_chain, compose, make_dict, attach_info, hook

__protobuf__ = ub.__protobuf__

from .logging import debug

log = logging.Logger(__name__)


class Scheduler(util.CoroutineWrapper):
    def __init__(self,
                 callback: t.Callable,
                 inputs: t.Iterable[t.Callable[[], t.Awaitable]],
                 mode: ub.ProcessingMode,
                 loop: asyncio.BaseEventLoop | None = None):
        self._loop = loop or asyncio.get_running_loop()
        self._callback = None
        self._inputs = inputs
        self.collected_inputs: t.Dict = {}
        self._delta_time = None
        self.done = []
        self.pending = []
        self.mode = mode
        self.callback = callback
        super().__init__(coroutine=self._run())

    @property
    def callback(self) -> t.Callable:
        return self._callback

    @callback.setter
    def callback(self, value):
        log.debug(f"Changed callback in {self} from {self._callback} to {value}")
        last_time = self._loop.time()

        def _write_timestamp():
            nonlocal last_time
            time = self._loop.time()
            self._delta_time = time - last_time
            last_time = time

        self._callback = function_chain(_write_timestamp, value)

    @property
    def delta_time(self):
        return self._delta_time

    def _run(self):
        if self.mode.frequency:
            delay = 1.0 / self.mode.frequency.hertz
        if self.mode.trigger_on_input:
            delay = self.mode.trigger_on_input.min_delay_ms / 1000
        if self.mode.lockstep:
            raise NotImplementedError(f"Not implemented yet")

        async def _trigger():
            while True:
                start_time = self._loop.time()
                self.done, self.pending = await asyncio.wait(
                    [get() for get in self._inputs],
                    timeout=(delay if self.mode.frequency else None),
                    return_when=(
                        asyncio.ALL_COMPLETED
                        if self.mode.trigger_on_input and self.mode.trigger_on_input.all_inputs_need_update else
                        asyncio.FIRST_COMPLETED
                    )
                )
                end_time = self._loop.time()
                remaining = start_time + delay - end_time
                if remaining < 0:
                    self._loop.call_soon(callback=self._callback)
                else:
                    self._loop.call_later(delay=remaining, callback=self._callback)

        return _trigger()


class ProcessingRoutine(util.CoroutineWrapper, ub.ProcessingModule, metaclass=ProtoRegistry):
    __unique_key_attr__ = 'name'

    def __init__(self, mapping=None, eval_strings=False, **kwargs):
        # we allow initialisation from ub.ProcessingModule Wrappers
        if isinstance(mapping, ub.ProcessingModule):
            mapping = ub.ProcessingModule.pb(mapping)

        self._protocol = ProcessingProtocol(pm=self)
        self._change_specs = asyncio.Condition()
        super().__init__(coroutine=protocol.RunProtocol(protocol=self._protocol), mapping=mapping, **kwargs)

        if eval_strings or debug():
            suffix = '_stringified'
            string_fields = [
                name for name in ub.ProcessingModule.pb(self).DESCRIPTOR.fields_by_name
                if name.endswith(suffix) and getattr(self, name)
            ]
            definitions = map(partial(getattr, self), string_fields)
            for name, source in zip(string_fields, definitions):
                created_name = name[:-len(suffix)]
                ns = {}
                exec(source, {}, ns)
                func = ns.get(created_name)
                if not func or not callable(func):
                    raise ValueError(f"Evaluating {name} did not create a function named {created_name}")

                setattr(type(self), created_name, func)

        self.validate()
        self._input_topic_getter = hook(lambda _: None)
        self._output_topic_getter = hook(lambda _: None)

    @property
    def change_specs(self) -> asyncio.Condition:
        return self._change_specs

    @property
    def get_input_topic(self) -> hook[t.Callable[[ub.ModuleIO], topics.Topic | None]]:
        return self._input_topic_getter

    @property
    def get_output_topic(self) -> hook[t.Callable[[ub.ModuleIO], topics.Topic | None]]:
        return self._output_topic_getter

    async def apply_io_mapping(self,
                               io_mapping: ub.IOMapping,
                               topic_map: t.Mapping[str, topics.Topic]):

        get_name = (lambda io: io.internal_name)
        get_input_mapping = {mapping.input_name: mapping for mapping in io_mapping.input_mappings}.get
        get_output_mapping = {mapping.output_name: mapping for mapping in io_mapping.output_mappings}.get
        get_topic = (lambda mapping: topic_map.get(mapping.topic or mapping.topic_mux))

        _input_decorators = self.get_input_topic.decorators
        _output_decorators = self.get_output_topic.decorators

        self._input_topic_getter = hook(compose(get_name, get_input_mapping, get_topic))
        self._output_topic_getter = hook(compose(get_name, get_output_mapping, get_topic))

        # carry over decorators
        list(map(self._input_topic_getter.register_decorator, _input_decorators))
        list(map(self._output_topic_getter.register_decorator, _output_decorators))

        if io_mapping.output_mappings or io_mapping.input_mappings:
            async with self.change_specs:
                self.change_specs.notify_all()

    @hook
    def on_created(self, context):
        pass

    @hook
    def on_processing(self, context):
        pass

    @hook
    def on_halted(self, context):
        pass

    @hook
    def on_destroyed(self, context):
        pass

    @hook
    def on_init(self, context):
        pass

    def validate(self):
        for rule in self.validation_rules:
            rule(self)

    def _validate_language(self):
        if self.language and self.language != self.Language.PY:
            raise ValueError(
                f"{self} can only run Python processing modules. language {self.language!r} specified."
            )

        self.language = self.Language.PY

    def _validate_id(self):
        if self.id is None:
            raise ValueError(
                f"id of {self} is not set"
            )

    validation_rules: t.List[t.Callable[[ProcessingRoutine], None]] = [
        _validate_language,
        _validate_id,
    ]


class PM_STAT(enum.IntFlag):
    INITIALIZED = enum.auto()
    CREATED = enum.auto()
    PROCESSING = enum.auto()
    HALTED = enum.auto()
    DESTROYED = enum.auto()


class ProcessingProtocol(protocol.UbiiProtocol[ub.ProcessingModule.Status]):
    starting_state = PM_STAT.INITIALIZED
    end_state = PM_STAT.DESTROYED
    AnyState = PM_STAT.INITIALIZED | PM_STAT.CREATED | PM_STAT.PROCESSING | PM_STAT.HALTED | PM_STAT.DESTROYED

    def __init__(self, pm: ProcessingRoutine):
        super().__init__()
        self.pm: ProcessingRoutine = pm

    class pm_proxy:
        _Status = t.Union[PM_STAT, ub.ProcessingModule.Status]

        @classmethod
        def _get_decorator(cls, callable: t.Callable):
            def decorator(func):
                @wraps(func)
                async def _inner(instance: ProcessingProtocol, context):
                    result = callable(instance.pm, context)
                    if asyncio.iscoroutine(result):
                        await result
                    return await func(instance, context)

                return _inner

            return decorator

        @classmethod
        def set_status_in_pm(cls, status):
            async def set_status(pm: ProcessingRoutine, _):
                async with pm.change_specs:
                    pm.status = status
                    pm.change_specs.notify_all()

            return cls._get_decorator(set_status)

        @classmethod
        def callback_in_pm(cls, name: str):
            def callback(pm: ProcessingRoutine, context):
                _cb = getattr(pm, name)
                assert callable(_cb)
                return _cb(context)

            return cls._get_decorator(callback)

    @pm_proxy.callback_in_pm('on_created')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.CREATED)
    async def on_created(self, context):
        """
        Blahblah
        :param context:
        :return:
        """
        log.info(f"created processing module {self.pm}")
        await context.trigger_processing.wait()
        context.delta_time = context.scheduler.delta_time
        context.inputs = types.SimpleNamespace(**{
            result.info[0]: getattr(result.value, result.info[1])
            for result in map(lambda task: task.result(), context.scheduler.done)
        })

        not_set = object()
        fields = [(out.internal_name, t.Any, dataclasses.field(default=not_set)) for out in self.pm.outputs]
        outputs = dataclasses.make_dataclass('outputs', fields, init=True)  # noqa
        context.outputs = outputs()

        async with self.pm.change_specs:
            await self.pm.change_specs.wait_for(lambda: self.pm.get_output_topic.decorators)

        context.publish_queue = asyncio.Queue()

        async def publish_task():
            while True:
                publish_call = await context.publish_queue.get()
                await publish_call
                context.publish_queue.task_done()

        context.nursery.create_task(publish_task())

        await self.state.set(PM_STAT.PROCESSING)

    @pm_proxy.callback_in_pm('on_processing')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.PROCESSING)
    async def on_processing(self, context):
        def write_timestamp_to_context(scheduler: Scheduler, ctx):
            ctx.delta_time = scheduler.delta_time

        def publish_outputs_to_topics(pm, ctx):
            get_output_io = {io.internal_name: io for io in pm.outputs}.get
            for io, value in zip(map(get_output_io, vars(ctx.outputs)), vars(ctx.outputs).values()):
                topic, publish = pm.get_output_topic(io)
                ctx.publish_queue.put_nowait(publish({'topic': topic.pattern, io.message_format: value}))

        context.scheduler.callback = function_chain(
            partial(write_timestamp_to_context, context.scheduler, context),
            partial(self.pm.on_processing, context),
            partial(publish_outputs_to_topics, self.pm, context),
        )

    @pm_proxy.callback_in_pm('on_halted')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.HALTED)
    async def on_halted(self, context):
        pass

    @pm_proxy.callback_in_pm('on_destroyed')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.DESTROYED)
    async def on_destroyed(self, context):
        pass

    @pm_proxy.callback_in_pm('on_init')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.INITIALIZED)
    async def on_init(self, context):
        # use the Processing Protocol as a task nursery if possible
        nursery = util.TaskNursery.registry.get('ubii.interact.processing.ProcessingProtocol_0', None)
        context.loop = asyncio.get_running_loop()
        context.nursery = nursery or context.loop

        async with self.pm.change_specs:
            inputs = await self.pm.change_specs.wait_for(
                partial(
                    make_dict(key=lambda io: (io.internal_name, io.message_format),
                              value=self.pm.get_input_topic,
                              filter_none=True),
                    self.pm.inputs
                )
            )

        context.trigger_processing = asyncio.Event()
        context.scheduler = Scheduler(
            callback=context.trigger_processing.set,
            inputs=[attach_info((name, msg_fmt), topic.buffer.get) for (name, msg_fmt), topic in inputs.items()],
            mode=self.pm.processing_mode
        )

        context.nursery.create_task(context.scheduler)
        await self.state.set(PM_STAT.CREATED)

    state_changes = {
        (None, PM_STAT.INITIALIZED): on_init,
        (AnyState, PM_STAT.CREATED): on_created,
        (AnyState, PM_STAT.PROCESSING): on_processing,
        (AnyState, PM_STAT.HALTED): on_halted,
        (AnyState, PM_STAT.DESTROYED): on_destroyed,
    }
