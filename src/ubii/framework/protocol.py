from __future__ import annotations

import abc
import asyncio
import functools
import logging
import types
import warnings
from typing import (
    Awaitable,
    Generic,
    Coroutine,
    Mapping,
    Any,
    Callable,
    Tuple,
)

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

from . import util
from .util.typing import (
    T_EnumFlag,
    Descriptor,
)

Callback = Callable[..., Coroutine[Any, Any, None]]
_StateChange = Tuple[T_EnumFlag, T_EnumFlag]

log = logging.getLogger(__name__)


class AbstractProtocol(Generic[T_EnumFlag], abc.ABC):
    """
    ABC to implement a state machine with async callbacks. A :class:`AbstractProtocol` can be used as an async context
    manager, to `start` and `stop` the protocol automatically, or be "manually" started and stopped using the protocols
    methods.
    """

    state_changes: Mapping[Tuple[T_EnumFlag | None, ...], Callback]
    """
    Assign to this mapping in your concrete implementation to define callbacks for state changes.
    """
    starting_state: T_EnumFlag
    """
    Assign a state to this attribute as the protocols starting state when implementing a concrete protocol
    """
    end_state: T_EnumFlag
    """
    Assign a state to this attribute as the protocols end state when implementing a concrete protocol
    """

    @cached_property
    def context(self) -> types.SimpleNamespace:
        """
        It's encouraged to overwrite this and return a dataclass or something that is better type-able  in your
        concrete implementation
        """
        return types.SimpleNamespace()

    def __init__(self) -> None:
        super().__init__()
        self.__name__: str = self.__class__.__name__
        self._state = None
        self._run = None
        self.change_context = asyncio.Condition()
        """
        Condition to notify / wait for changed :attr:`.context`
        """
        self.task_nursery = util.TaskNursery(name=f"Task Nursery for {self}")
        """
        This manages the tasks created by the protocol as well as teardown behaviour if the protocol is stopped
        """
        self.get_state_change_callback = functools.partial(util.enum.EnumMatcher.get_matching_value,
                                                           mapping=self.state_changes)
        """
        This callable uses :meth:`~ubii.framework.util.EnumMatcher.get_matching_value`
        to return the appropriate state change from :attr:`.state_changes` for a tuple of states
        """

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
    state: util.condition_property = util.condition_property(fget=_get_state, fset=_set_state)
    """
    Makes shared access to the protocol state easy
    """

    def start(self: AbstractProtocol[T_EnumFlag]) -> AbstractProtocol[T_EnumFlag]:
        """
        Starts the protocol task.

        It is encouraged to wait for the protocol if the task is cancelled instead of waiting for the task.
        In either case, canceling the task will trigger the sentinel task to handle the
        protocol teardown, which might - depending on the protocol implementation - mean that closing the event loop
        shortly after the cancellation might raise exceptions from the scheduled cleanup operations, so it's
        mandatory to sleep for a short time (e.g. asyncio.sleep(0)) before closing the loop if your protocol
        schedules async tasks during its teardown.

        Returns:
            reference to `self` -- the started protocol

        See Also:
            :class:`RunProtocol` -- specialized coroutine that is used to create the task

            :attr:`.task_nursery` -- the object that handles cancellation / stopping of the task when
            necessary
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
        This will call the appropriate state change callback (for the :math:`current \\rightarror end` state change)
        and finish the task created by :meth:`.start`

        Returns control back to the caller after the task created by :meth:`.start` stopped and all teardown
        callbacks have been scheduled
        """
        assert self._run, "Protocol not running, `start()` first"
        await self.state.set(self.end_state)
        await self._run

    async def __aenter__(self):
        self.start()
        await self.state.get(predicate=lambda value: value == self.starting_state)
        return self

    def __aexit__(self, *exc_info):
        return self.task_nursery.__aexit__(*exc_info)


@util.dunder.repr('protocol')
class RunProtocol(util.CoroutineWrapper):
    """
    A specialized coroutine to wait for changing protocol :attr:`~AbstractProtocol.state`, get the appropriate callbacks
    from the protocols :attr:`~AbstractProtocol.state_changes` and execute them.

    Starting a protocol using the :meth:`~AbstractProtocol.start` method, starts a :class:`RunProtocol` coroutine
    as a task using the protocol's :attr:`AbstractProtocol.task_nursery`.
    """

    def __init__(self, protocol: AbstractProtocol):
        """
        Args:
            protocol: a protocol to run
        """
        self.protocol = protocol
        """
        Reference to the protocol that is running in this coroutine
        """
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

        if not isinstance(coro, Awaitable):
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
                # wait until a state is set that is not the current state
                current = await self.protocol.state.get(
                    predicate=lambda value: value != current
                )

        log.debug(f"{self} finished.")
