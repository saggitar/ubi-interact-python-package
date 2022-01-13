from __future__ import annotations

import abc
import asyncio
import logging
import types
import typing as t
import warnings
from contextlib import AsyncExitStack
from functools import partial, cached_property
from itertools import product

from . import topics, constants, client
from .. import util
from ..util.typing import _T_EnumFlag, _Descriptor, _Decorator

Callback = t.Callable[..., t.Coroutine[t.Any, t.Any, None]]
_StateChange = t.Tuple[_T_EnumFlag, _T_EnumFlag]

log = logging.getLogger(__name__)


class UbiiProtocol(t.Generic[_T_EnumFlag], util.TaskNursery, abc.ABC):
    __name__: str

    @classmethod
    @property
    @abc.abstractmethod
    def state_changes(cls) -> t.Mapping[t.Tuple[_T_EnumFlag | None, ...], Callback]:
        ...

    @classmethod
    @property
    @abc.abstractmethod
    def starting_state(cls) -> _T_EnumFlag:
        ...

    @classmethod
    @property
    @abc.abstractmethod
    def end_state(cls) -> _T_EnumFlag:
        ...

    @cached_property
    def context(self):
        return types.SimpleNamespace()

    def __init__(self) -> None:
        super().__init__()
        self.__name__ = type(self).__name__
        self.change_context = asyncio.Condition()
        self._state = self.starting_state
        self._run = None
        self._get_state_change_callback = partial(util.EnumMatcher.get_matching_value, mapping=self.state_changes)

    def _get_state(self) -> _T_EnumFlag:
        return self._state

    def _set_state(self, new_state: _T_EnumFlag):
        current = self._state
        if new_state == current:
            return

        # it is allowed to change the state if a matching callback is defined
        if not self._get_state_change_callback((current, new_state), None):
            raise ValueError(f"Can't change state {current!r} -> {new_state!r}")

        self._state = new_state

    # declaring the property this way helps pycharm to infer types correctly see
    # https://youtrack.jetbrains.com/issue/PY-15176 and related issues.
    state = util.condition_property(fget=_get_state, fset=_set_state)

    def run(self):
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
            self._run = self.create_task(RunProtocol(self))
            # clean up when the task finishes
            self._run.add_done_callback(lambda _: self.trigger_sentinel.set())
        else:
            warnings.warn(f"{self} already running.")

        return self._run

    def __await__(self):
        with warnings.catch_warnings():
            # this might not be the first call to run() but we know this, so it's ok.
            warnings.simplefilter('ignore', UserWarning)
            yield from self.run().__await__()
        # try to complete the teardown (callbacks might schedule tasks i.e. it's not safe to assume that everything
        # is torn down fully after the sentinel task completed).
        yield from self.sentinel.__await__()

    async def stop(self) -> None:
        """
        Gracefully shut down the protocol by setting the protocol state to the end state.
        This will call appropriate state change callbacks (make sure those are defined, else ``stop`` will raise an
        exception) and finish the ``run()`` task, which will in turn trigger the sentinel task to handle the
        protocol teardown.

        Stop returns control back to the caller after the ``run()`` task stopped and all teardown callbacks
        have been scheduled.
        """
        await self.state.set(self.end_state)
        await self

    async def __aenter__(self):
        # run the protocol
        _ = self.run()
        return self

    async def __aexit__(self, *exc_infos):
        await self.stop()


class RunProtocol(util.CoroutineWrapper):

    def __init__(self, protocol: UbiiProtocol):
        self._protocol = protocol
        super().__init__(coroutine=self._run())
        self.__name__ = self._protocol.__name__

    async def _no_callback_found(self, protocol, context):
        raise RuntimeError(f"No callback found for context {context} in {protocol}")

    async def _run(self):
        previous = None
        end_state = self._protocol.end_state
        current = self._protocol.starting_state
        context = self._protocol.context
        get_state = partial(util.EnumMatcher.get_matching_value, mapping=self._protocol.state_changes)

        while previous != end_state:
            callback = get_state((previous, current), self._no_callback_found)
            context.state_change = (previous, current)
            log.debug(f"Changed state {previous!r}->{current!r} in {self._protocol}, got callback {callback}")

            # also functions are descriptors
            assert isinstance(callback, _Descriptor), f"{callback} needs to be a descriptor, e.g. a function"
            coro = callback.__get__(self._protocol, type(self._protocol))(context)

            if not asyncio.iscoroutine(coro):
                raise RuntimeError(f"{callback} did not return a coroutine (not using `async def`?)")

            async with self._protocol.change_context:
                await coro
                self._protocol.change_context.notify_all()

            previous = current
            if current != end_state:
                # wait until a state is set that is not the previous state
                current = await self._protocol.state.get(
                    predicate=lambda value: value != current
                )


class StandardProtocol(UbiiProtocol[_T_EnumFlag], t.Generic[_T_EnumFlag], abc.ABC):
    _get_name = (lambda h: h.__name__)  # type: ignore
    hook_function = util.registry(_get_name, util.hook)
    __hooks__: t.Set[_Decorator] = set()

    def __init__(self, config: constants.UbiiConfig = constants.GLOBAL_CONFIG, log: logging.Logger | None = None):
        super().__init__()
        self.config = config
        self.log = log or logging.getLogger(__name__)
        self.client: client.UbiiClient | None = None
        self.exit_stack = AsyncExitStack()
        self.add_sentinel_callback(self.exit_stack.aclose())
        self.fallback_handler = util.function_chain(
            self.log.exception,
            lambda _: self.trigger_sentinel.set()
        )

    @abc.abstractmethod
    async def create_service_map(self, context):
        """
        Create a ServiceMap in the context as ``context.service_map`` which has to be able to make a single
        service call: ``server_config`` (see documentation).
        """

    @abc.abstractmethod
    async def update_config(self, context):
        """
        Update the server configuration in the context.
            *   ``context.server`` is a ``ub.Server`` message with the configuration of the master node,
            *   ``context.constants``  is a ``ub.Constants`` message of the default constants of the server
        """

    @abc.abstractmethod
    async def update_services(self, context):
        """
        Update the service map in the context. Make sure ``context.service_map`` is able to perform all
        service calls advertised by the master node after this coroutine completes.
        """

    @abc.abstractmethod
    async def create_client(self, context):
        """
        Create a client in the context. ``context.client`` typically is a ``ub.Client`` wrapper, e.g. a UbiiClient
        which at this moment is not expected to be fully functional.
        """

    @abc.abstractmethod
    def register_client(self, context) -> t.AsyncContextManager[None]:
        """
        Create a context manager to register the ``context.client`` client, and unregister it when the protocol stops.
        After successful registration the context manager needs to set the protocol state to ``UbiiStates.REGISTERED``.
        The ``context.client`` is expected to be up-to-date after registration.
        """

    @abc.abstractmethod
    async def create_topic_connection(self, context):
        """
        It's expected that ``context.topic_connection`` is a fully functional topic connection after this coroutine
        is completed.
        """

    @abc.abstractmethod
    async def implement_client(self, context):
        """
        Make sure the ``context.client`` has fully implemented behaviour. The context at this point should contain
        a service_map and a topic_connection. It's expected that ``context.client`` can be awaited after this
        coroutine is finished, which returns a fully functional client.
        """

    @hook_function
    async def on_start(self, context):
        await self.create_service_map(context)
        await self.update_config(context)
        await self.update_services(context)
        await self.create_client(context)
        await self.exit_stack.enter_async_context(self.register_client(context))

    @hook_function
    async def on_registration(self, context):
        await self.create_topic_connection(context)
        await self.implement_client(context)
        try:
            # make sure client is implemented
            context.client = await asyncio.wait_for(context.client, timeout=5)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Client is not implemented")

    @hook_function
    async def on_connect(self, context):
        splitter = self.create_task(
            topics.StreamSplitRoutine(container=context.topic_store, stream=context.topic_connection)
        )
        # when the splitter task is finished (e.g. when the topic connection closes), stop the protocol
        splitter.add_done_callback(lambda _: self.trigger_sentinel.set())

    @hook_function
    async def on_stop(self, context):
        self.log.info(f"Stopped protocol {self}")

    def __init_subclass__(cls, **kwargs):
        """
        Register decorators
        """
        hook_function: util.hook
        for hook_function, hk in product(cls.hook_function.registry.values(), cls.__hooks__):
            hook_function.register_decorator(hk)

        super().__init_subclass__(**kwargs)
