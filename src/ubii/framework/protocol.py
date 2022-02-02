from __future__ import annotations

import abc
import asyncio
import logging
import types
import typing as t
import warnings
from functools import partial

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

from . import (
    util as util_,
)
from .util.typing import T_EnumFlag, Descriptor

Callback = t.Callable[..., t.Coroutine[t.Any, t.Any, None]]
_StateChange = t.Tuple[T_EnumFlag, T_EnumFlag]

log = logging.getLogger(__name__)


class AbstractProtocol(t.Generic[T_EnumFlag], abc.ABC):
    state_changes: t.Mapping[t.Tuple[T_EnumFlag | None, ...], Callback]
    starting_state: T_EnumFlag
    end_state: T_EnumFlag

    @cached_property
    def context(self):
        return types.SimpleNamespace()

    def __init__(self) -> None:
        super().__init__()
        self.__name__: str = self.__class__.__name__
        self.change_context = asyncio.Condition()
        self._state = None
        self._run = None
        self.task_nursery = util_.TaskNursery(name=f"Task Nursery for {self}")
        self.get_state_change_callback = partial(util_.enum.EnumMatcher.get_matching_value,
                                                 mapping=self.state_changes)

    def _get_state(self) -> T_EnumFlag:
        return self._state

    def _set_state(self, new_state: T_EnumFlag):
        current = self._state
        if new_state == current:
            return

        # it is allowed to change the state if a matching callback is defined
        if not self.get_state_change_callback((current, new_state), None):
            raise ValueError(f"Can't change state {current!r} -> {new_state!r}")

        self._state = new_state

    # declaring the property this way helps pycharm to infer types correctly see
    # https://youtrack.jetbrains.com/issue/PY-15176 and related issues.
    state = util_.condition_property(fget=_get_state, fset=_set_state)

    def start(self):
        """
        Start the protocol
        :return: Task running the protocol.

        It is encouraged to wait for the protocol if the task is cancelled instead of waiting for the task.
        In either case, canceling the task will trigger the sentinel task to handle the
        protocol teardown, which might - depending on the protocol implementation - mean that closing the event loop
        shortly after the cancellation might raise exceptions from the scheduled cleanup operations, so it's
        mandatory to sleep for a short time (e.g. asyncio.sleep(0)) before closing the loop if your protocol
        schedules async tasks during its teardown.
        """
        if not self._run:
            self._run = self.task_nursery.create_task(RunProtocol(self))
        else:
            warnings.warn(f"{self} already running.")

        self.task_nursery.push_async_callback(self.stop)
        return self

    async def stop(self) -> None:
        """
        Gracefully shut down the protocol by setting the protocol state to the end state.
        This will call appropriate state change callbacks (make sure those are defined, else ``stop`` will raise an
        exception) and finish the ``run()`` task.

        Stop returns control back to the caller after the ``run()`` task stopped and all teardown callbacks
        have been scheduled.
        """
        assert self._run, "Protocol not running, `start()` first"
        await self.state.set(self.end_state)
        await self._run

    async def __aenter__(self):
        self.start()
        return self

    def __aexit__(self, *exc_info):
        return self.task_nursery.__aexit__(*exc_info)


class RunProtocol(util_.CoroutineWrapper):

    def __init__(self, protocol: AbstractProtocol):
        self.protocol = protocol
        self.__name__ = repr(self.protocol)
        super().__init__(coroutine=self._run())

    @staticmethod
    async def _no_callback_found(protocol, context):
        raise RuntimeError(f"No callback found for state_change {context.state_change} in {protocol}")

    async def _run_state_change_callback(self, prev, cur, ctx):
        cb = self.protocol.get_state_change_callback((prev, cur), self._no_callback_found)
        ctx.state_change = (prev, cur)
        log.debug(f"Changed state {prev!r}->{cur!r} in {self.protocol}, got callback {getattr(cb, '__name__', cb)!r}")

        # all functions are also descriptors
        assert isinstance(cb, Descriptor), f"{cb} needs to be a descriptor, e.g. a function"
        coro = cb.__get__(self.protocol, type(self.protocol))(ctx)

        if not isinstance(coro, t.Awaitable):
            raise RuntimeError(f"{cb} is not awaitable (not using `async def`?)")

        return await coro

    async def _run(self):
        await self.protocol.state.set(self.protocol.starting_state)
        previous = None
        current = self.protocol.state.value
        end_state = self.protocol.end_state
        context = self.protocol.context

        while previous != end_state:
            async with self.protocol.change_context:
                try:
                    await self._run_state_change_callback(previous, current, context)
                    self.protocol.change_context.notify_all()
                except Exception as initial:
                    if self.protocol.state.value == current:
                        raise
                    else:
                        previous = current
                        current = self.protocol.state.value

                        log.debug(f"Changed state during exception {initial}")
                        try:
                            result = await self._run_state_change_callback(previous, current, context)
                        except Exception as nested:
                            raise nested from initial
                        else:
                            if not result:
                                raise initial

            previous = current
            if current != end_state:
                # wait until a state is set that is not the previous state
                current = await self.protocol.state.get(
                    predicate=lambda value: value != previous
                )

        log.debug(f"{self} finished.")


