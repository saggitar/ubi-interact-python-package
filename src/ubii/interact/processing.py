from __future__ import annotations

import logging

import asyncio
import enum
import typing as t
from functools import wraps, cached_property, partial
from warnings import warn

import codestare.async_utils as util
import ubii.proto as ub
from . import protocol, topics, _util
from ._util import ProtoRegistry
from .logging import ProtoFormatMixin

__protobuf__ = ub.__protobuf__

log = logging.Logger(__name__)


class Scheduler(util.CoroutineWrapper):
    def __init__(self,
                 condition: asyncio.Condition,
                 inputs: t.Dict[str, asyncio.Queue],
                 mode: ub.ProcessingMode,
                 loop: asyncio.BaseEventLoop | None = None):
        self.mode = mode
        self._condition = condition
        self._inputs = inputs
        self._loop = loop or asyncio.get_running_loop()

        self.collected_inputs: t.Dict = {}
        super().__init__(coroutine=self._run())

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
                done, pending = await asyncio.wait(
                    [input_topic.get() for input_topic in self._inputs.values()],
                    return_when=(
                        asyncio.ALL_COMPLETED
                        if self.mode.trigger_on_input and self.mode.trigger_on_input.all_inputs_need_update else
                        asyncio.FIRST_COMPLETED
                    )
                )
                print()

        return _trigger()


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

    class status_proxy:
        _Status = t.Union[PM_STAT, ub.ProcessingModule.Status]

        @classmethod
        def set_status_in_pm(cls, status):
            def decorator(func):
                @wraps(func)
                async def _inner(*args, **kwargs):
                    instance: ProcessingProtocol = args[0]
                    instance.pm.status = status
                    return await func(*args, **kwargs)

                return _inner

            return decorator

    @status_proxy.set_status_in_pm(ub.ProcessingModule.Status.CREATED)
    async def on_created(self, context):
        """
        Blahblah
        :param context:
        :return:
        """
        log.info(f"created processing module {self.pm}")
        _trigger_processing = asyncio.Condition()

        context.nursery.create_task(
            Scheduler(condition=_trigger_processing, inputs=context.inputs, mode=self.pm.processing_mode)
        )

    @status_proxy.set_status_in_pm(ub.ProcessingModule.Status.PROCESSING)
    async def on_processing(self, context):
        """
        Blahblah
        :param context:
        :return:
        """

    @status_proxy.set_status_in_pm(ub.ProcessingModule.Status.HALTED)
    async def on_halted(self, context):
        """
        Blahblah
        :param context:
        :return:
        """

    @status_proxy.set_status_in_pm(ub.ProcessingModule.Status.DESTROYED)
    async def on_destroyed(self, context):
        """
        Blahblah
        :param context:
        :return:
        """

    @status_proxy.set_status_in_pm(ub.ProcessingModule.Status.INITIALIZED)
    async def on_init(self, context):
        """

        :param context:
        :type context:
        """
        # use the Processing Protocol as a task nursery if possible
        nursery = util.TaskNursery.registry.get('ubii.interact.processing.ProcessingProtocol_0', None)
        context.loop = asyncio.get_running_loop()
        context.nursery = nursery or context.loop

        async with self.pm.change_specs:
            await self.pm.change_specs.wait_for(lambda: self.pm.input_mapping or self.pm.output_mapping)

        input_topics = self.pm.input_mapping
        output_topics = self.pm.output_mapping

        # pull topic data into context whenever available, let scheduler decide what to do with it.
        async def put(topic: topics.Topic, queue: asyncio.Queue):
            async for data in topic:
                await queue.put(data)

        context.inputs = {name: asyncio.Queue() for name, topic in input_topics.items()}
        context.outputs = {name: asyncio.Queue() for name, topic in output_topics.items()}

        for name, queue in context.inputs.items():
            context.nursery.create_task(put(topic=input_topics[name], queue=queue))

        for name, queue in context.outputs.items():
            context.nursery.create_task(put(topic=output_topics[name], queue=queue))

        await self.state.set(PM_STAT.CREATED)

    state_changes = {
        (None, PM_STAT.INITIALIZED): on_init,
        (AnyState, PM_STAT.CREATED): on_created,
        (AnyState, PM_STAT.PROCESSING): on_processing,
        (AnyState, PM_STAT.HALTED): on_halted,
        (AnyState, PM_STAT.DESTROYED): on_destroyed,
    }


class ProcessingRoutine(util.CoroutineWrapper, ub.ProcessingModule, metaclass=ProtoRegistry):
    __unique_key_attr__ = 'name'

    def __init__(self, mapping=None, **kwargs):
        # we allow initialisation from ub.ProcessingModule Wrappers
        if isinstance(mapping, ub.ProcessingModule):
            mapping = ub.ProcessingModule.pb(mapping)

        self._protocol = ProcessingProtocol(pm=self)
        super().__init__(coroutine=protocol.RunProtocol(protocol=self._protocol), mapping=mapping, **kwargs)

        self._inputs: t.MutableMapping[str, topics.Topic] = {}
        self._outputs: t.MutableMapping[str, topics.Topic] = {}
        self._change_specs = asyncio.Condition()
        self.validate()

    @property
    def change_specs(self) -> asyncio.Condition:
        return self._change_specs

    @property
    def input_mapping(self) -> t.Mapping[str, topics.Topic]:
        return self._inputs

    @property
    def output_mapping(self) -> t.Mapping[str, topics.Topic]:
        return self._outputs

    @classmethod
    def apply_io_mapping(cls, io_mapping: ub.IOMapping, topic_map: t.Mapping[str, topics.Topic]):
        assert io_mapping.processing_module_id is not None
        instance: ProcessingRoutine | None = cls.registry.get(io_mapping.processing_module_id)
        if not instance:
            warn(f"no processing module found for {io_mapping}")
            return

        if io_mapping.input_mappings:
            input_mapping: ub.TopicInputMapping
            instance._inputs = {
                input_mapping.input_name: topic_map[input_mapping.topic or input_mapping.topic_mux]
                for input_mapping in io_mapping.input_mappings
            }
        if io_mapping.output_mappings:
            output_mapping: ub.TopicOutputMapping
            instance._outputs = {
                output_mapping.output_name: topic_map[output_mapping.topic or output_mapping.topic_mux]
                for output_mapping in io_mapping.output_mappings
            }

        if instance._outputs or instance._inputs:
            instance.change_specs.notify_all()

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
