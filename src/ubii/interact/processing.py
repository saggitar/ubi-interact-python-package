from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import types
import typing as t
from functools import wraps, partial

import ubii.proto as ub
from . import protocol, topics
from .logging import debug
from .. import util

__protobuf__ = ub.__protobuf__
log = logging.Logger(__name__)


class Scheduler(util.CoroutineWrapper):
    """
    A Scheduler is a wrapper around the coroutine created by its ``get_trigger_loop`` method.
    A scheduler - depending on the used ProcessingMode ``mode`` - waits for a certain delay and / or until
    any or all of its inputs are finished and then schedules its callback.

    Input callables are expected to accept a boolean argument, indicating that the scheduler needs to
    wait on the inputs, or
    """

    def __init__(self,
                 callback: t.Callable[..., t.Any],
                 inputs: t.Iterable[t.Callable[[], t.Awaitable]],
                 mode: ub.ProcessingMode):
        self._loop = asyncio.get_running_loop()
        self._callback = None
        self._inputs = inputs
        self._delta_time = None

        self.done: t.List[t.Awaitable] = []
        self.pending: t.List[t.Awaitable] = []
        self.mode = mode
        self.callback = callback

        super().__init__(coroutine=self.get_trigger_loop())

    @property
    def callback(self) -> t.Callable[..., t.Any] | None:
        return self._callback

    @callback.setter
    def callback(self, value):
        """
        The actual internal callback also calculates ``delta_time`` whenever it's called.
        """
        log.debug(f"Changed callback in {self} from {self._callback} to {value}")
        last_time = self._loop.time()

        def _write_timestamp():
            nonlocal last_time
            time = self._loop.time()
            self._delta_time = time - last_time
            last_time = time

        self._callback = util.function_chain(_write_timestamp, value)

    @property
    def delta_time(self):
        return self._delta_time

    def get_trigger_loop(self):
        if self.mode.frequency:
            delay = 1.0 / self.mode.frequency.hertz
        if self.mode.trigger_on_input:
            delay = self.mode.trigger_on_input.min_delay_ms / 1000
        if self.mode.lockstep:
            raise NotImplementedError(f"Not implemented yet")

        async def _trigger_loop():
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

        return _trigger_loop()


class ProcessingRoutine(util.CoroutineWrapper, ub.ProcessingModule, metaclass=util.ProtoRegistry):
    __unique_key_attr__ = 'name'

    class helpers:
        @staticmethod
        def validate_language(pm: ProcessingRoutine):
            if pm.language and pm.language != pm.Language.PY:
                raise ValueError(f"{pm} can only run Python processing modules. language {pm.language!r} specified.")
            pm.language = pm.Language.PY

        @staticmethod
        def validate_id(pm: ProcessingRoutine):
            if pm.id is None:
                raise ValueError(f"id of {pm} is not set")

    def __init__(self, mapping=None, eval_strings=False, **kwargs):
        # we allow initialisation from ub.ProcessingModule Wrappers
        if isinstance(mapping, ub.ProcessingModule):
            mapping = ub.ProcessingModule.pb(mapping)

        self._protocol = ProcessingProtocol(pm=self)
        self._change_specs = asyncio.Condition()
        super().__init__(coroutine=protocol.RunProtocol(protocol=self._protocol), mapping=mapping, **kwargs)

        # eval stringified
        if eval_strings or debug():
            self._eval_string_funcs()

        self.validate()
        self._input_topic_getter = util.hook(lambda _: None)
        self._output_topic_getter = util.hook(lambda _: None)

    def _eval_string_funcs(self):
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

    @property
    def change_specs(self) -> asyncio.Condition:
        return self._change_specs

    @property
    def get_input_topic(self) -> util.hook[t.Callable[[ub.ModuleIO], topics.Topic | None]]:
        return self._input_topic_getter

    @property
    def get_output_topic(self) -> util.hook[t.Callable[[ub.ModuleIO], topics.Topic | None]]:
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

        self._input_topic_getter = util.hook(util.compose(get_name, get_input_mapping, get_topic))
        self._output_topic_getter = util.hook(util.compose(get_name, get_output_mapping, get_topic))

        # carry over decorators
        list(map(self._input_topic_getter.register_decorator, _input_decorators))
        list(map(self._output_topic_getter.register_decorator, _output_decorators))

        if io_mapping.output_mappings or io_mapping.input_mappings:
            async with self.change_specs:
                self.change_specs.notify_all()

    @util.hook
    def on_created(self, context):
        pass

    @util.hook
    def on_processing(self, context):
        pass

    @util.hook
    def on_halted(self, context):
        pass

    @util.hook
    def on_destroyed(self, context):
        pass

    @util.hook
    def on_init(self, context):
        pass

    def validate(self):
        for rule in self.validation_rules:
            rule(self)

    validation_rules: t.List[t.Callable[[ProcessingRoutine], None]] = [
        helpers.validate_language,
        helpers.validate_id,
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

    __output_not_set__ = object()

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

    class helpers:
        @staticmethod
        def write_scheduler_data_to_context(scheduler: Scheduler, ctx):
            ctx.delta_time = scheduler.delta_time
            inputs = vars(ctx.inputs)
            inputs.update(**{
                result.info[0]: getattr(result.value, result.info[1])
                for result in map(lambda task: task.result(), ctx.scheduler.done)
            })
            ctx.inputs = types.SimpleNamespace(**inputs)

        @staticmethod
        def start_publish_queue(ctx):
            ctx.publish_queue = asyncio.Queue()

            async def publish_task():
                while True:
                    publish_call = await ctx.publish_queue.get()
                    await publish_call
                    ctx.publish_queue.task_done()

            ctx.nursery.create_task(publish_task())

        @staticmethod
        def publish_outputs_to_topics(pm, ctx):
            get_output_io = {io.internal_name: io for io in pm.outputs}.get

            for io, value in zip(map(get_output_io, vars(ctx.outputs)), vars(ctx.outputs).values()):
                if value == ProcessingProtocol.__output_not_set__:
                    continue
                topic, publish = pm.get_output_topic(io)
                ctx.publish_queue.put_nowait(publish({'topic': topic.pattern, io.message_format: value}))

    def __init__(self, pm: ProcessingRoutine):
        super().__init__()
        self.pm: ProcessingRoutine = pm

    @pm_proxy.callback_in_pm('on_created')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.CREATED)
    async def on_created(self, context):
        """
        :param context:
        :return:
        """
        log.info(f"created processing module {self.pm}")
        # create inputs
        context.inputs = types.SimpleNamespace()

        # create outputs (use dataclass to get better error reporting)
        fields = [
            (out.internal_name, t.Any, dataclasses.field(default=self.__output_not_set__))
            for out in self.pm.outputs
        ]
        outputs = dataclasses.make_dataclass('outputs', fields, init=True)  # noqa
        context.outputs = outputs()

        # wait until publishing behaviour is added to output getter
        async with self.pm.change_specs:
            await self.pm.change_specs.wait_for(lambda: self.pm.get_output_topic.decorators)
        self.helpers.start_publish_queue(context)

        # wait until processing is triggered and change state to processing
        await context.trigger_processing.wait()
        self.helpers.write_scheduler_data_to_context(context.scheduler, context)
        await self.state.set(PM_STAT.PROCESSING)

    @pm_proxy.callback_in_pm('on_processing')
    @pm_proxy.set_status_in_pm(ub.ProcessingModule.Status.PROCESSING)
    async def on_processing(self, context):
        self.helpers.publish_outputs_to_topics(self.pm, context)

        # we just change the scheduler callback to get inputs and publish outputs.
        context.scheduler.callback = util.function_chain(
            partial(self.helpers.write_scheduler_data_to_context, context.scheduler, context),
            partial(self.pm.on_processing, context),
            partial(self.helpers.publish_outputs_to_topics, self.pm, context),
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
        # use the Processing Protocol as a task nursery if possible ?
        nursery = util.TaskNursery.registry.get('ubii.interact.processing.ProcessingProtocol_0', None)
        context.loop = asyncio.get_running_loop()
        context.nursery = nursery or context.loop

        # callable to create input_mapping dict from module io iterable
        make_input_dict: util.make_dict[t.Tuple[str, str], topics.Topic] = util.make_dict(
            key=lambda io: (io.internal_name, io.message_format),
            value=self.pm.get_input_topic,
            filter_none=True
        )

        # wait for applied input mapping
        async with self.pm.change_specs:
            inputs = await self.pm.change_specs.wait_for(partial(make_input_dict, self.pm.inputs))

        # start scheduler task
        context.trigger_processing = asyncio.Event()
        context.scheduler = Scheduler(
            callback=context.trigger_processing.set,
            inputs=[util.attach_info((name, msg_fmt), topic.buffer.get) for (name, msg_fmt), topic in inputs.items()],
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
