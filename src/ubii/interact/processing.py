from __future__ import annotations

import enum

from functools import wraps

import typing as t
import ubii.proto as ub
import codestare.async_utils as util

from .logging import ProtoFormatMixin
from . import protocol, topics, _util

__protobuf__ = ub.__protobuf__


class PM_STAT(enum.IntFlag):
    INITIALIZED = enum.auto()
    CREATED = enum.auto()
    PROCESSING = enum.auto()
    HALTED = enum.auto()
    DESTROYED = enum.auto()


class ProcessingProtocol(protocol.UbiiProtocol[ub.ProcessingModule.Status]):
    starting_state = PM_STAT.INITIALIZED
    end_state = PM_STAT.DESTROYED
    _Any = PM_STAT.INITIALIZED | PM_STAT.CREATED | PM_STAT.PROCESSING | PM_STAT.HALTED | PM_STAT.DESTROYED

    def __init__(self, pm: ProcessingRoutine):
        super().__init__()
        self.pm: ProcessingRoutine = pm

    class _set_status_in_pm_on_enter:
        def __init__(self, status):
            self.status = status

        def __call__(self, func):
            @wraps(func)
            async def _inner(*args, **kwargs):
                instance: ProcessingProtocol = args[0]
                await instance.pm.status_condition.set(self.status)
                return await func(*args, **kwargs)

            return _inner

    @_set_status_in_pm_on_enter(PM_STAT.CREATED)
    async def on_created(self, context):
        pass

    @_set_status_in_pm_on_enter(PM_STAT.PROCESSING)
    async def on_processing(self, context):
        pass

    @_set_status_in_pm_on_enter(PM_STAT.HALTED)
    async def on_halted(self, context):
        pass

    @_set_status_in_pm_on_enter(PM_STAT.DESTROYED)
    async def on_destroyed(self, context):
        pass

    @_set_status_in_pm_on_enter(PM_STAT.INITIALIZED)
    async def on_init(self, context):
        pass

    state_changes: _util.EnumMatchMapping = _util.EnumMatchMapping(mapping={
        (None, PM_STAT.INITIALIZED): on_init,
        (_Any, PM_STAT.CREATED): on_created,
        (_Any, PM_STAT.PROCESSING): on_processing,
        (_Any, PM_STAT.HALTED): on_halted,
        (_Any, PM_STAT.DESTROYED): on_destroyed,
    })


class ProcessingRoutine(util.CoroutineWrapper, ub.ProcessingModule, ProtoFormatMixin, metaclass=ub.ProtoMeta):
    def __init__(self, mapping=None,
                 *,
                 topic_map: t.Mapping[str, topics.Topic],
                 **kwargs):
        # we allow initialisation from ub.ProcessingModule Wrappers
        if isinstance(mapping, ub.ProcessingModule):
            mapping = ub.ProcessingModule.pb(mapping)

        self._protocol = ProcessingProtocol(pm=self)
        self._topic_map = topic_map
        super().__init__(coroutine=protocol.RunProtocol(protocol=self._protocol), mapping=mapping, **kwargs)

        self.validate()

    def _set_status(self, status: PM_STAT):
        self.status = status

    def _get_status(self) -> PM_STAT:
        return self.status

    status_condition = util.condition_property(fget=_get_status, fset=_set_status)

    def validate(self):
        for rule in self.validation_rules:
            rule(self)

    def _validate_language(self):
        if self.language and self.language != self.Language.PY:
            raise ValueError(
                f"{self} can only run Python processing modules. language {self.language!r} specified.")

        self.language = self.Language.PY

    validation_rules: t.List[t.Callable[[ProcessingRoutine], None]] = [
        _validate_language
    ]
