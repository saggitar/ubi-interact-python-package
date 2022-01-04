from __future__ import annotations

import asyncio
import enum
import logging
import types
import typing as t
import warnings
from itertools import chain

import codestare.async_utils as util


async def _handle_subscribe(self, *topics, as_regex=False, unsubscribe=False):
    service_topic = self._config.CONSTANTS.DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION

    message = {
        'client_id': self.id,
        f"{'un' if unsubscribe else ''}"
        f"{'subscribe_topic_regexp' if as_regex else 'subscribe_topics'}": topics
    }
    self.log.info(f"{self} subscribed to topic[s] {','.join(topics)}")
    await self.services[service_topic](**message)
    return tuple(self.topic_store[topic] for topic in topics)


_T_State = t.TypeVar('_T_State', bound=enum.IntEnum)
_T_Exception = t.TypeVar('_T_Exception', bound=Exception)
_T_co = t.TypeVar('_T_co', covariant=True)


class EnumMatchMapping(t.Mapping[t.Tuple[enum.IntEnum, ...], _T_co], t.Generic[_T_co]):
    def __init__(self: EnumMatchMapping[_T_co], mapping=t.Mapping[t.Tuple[enum.IntEnum, ...], _T_co]):
        self.data: t.Mapping[t.Tuple[enum.IntEnum, ...], _T_co] = mapping

    @staticmethod
    def match(base: t.Tuple[enum.IntEnum], query: t.Tuple[enum.IntEnum]):
        if not len(base) == len(query):
            return False

        if base == query:
            return True

        if any(x is None for x in chain(base, query)):
            return False

        return all(b & q == q for b, q in zip(base, query))

    def __getitem__(self, key: t.Tuple[enum.IntEnum, ...]) -> _T_co:
        matching = [value for enums, value in self.data.items() if EnumMatchMapping.match(enums, key)]
        if len(matching) != 1:
            raise KeyError(f"Found matching values {matching} for query {key}, not exactly one match")

        return matching[0]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> t.Iterator[_T_co]:
        return iter(self.data)


class UbiiProtocol(t.Generic[_T_State], util.TaskNursery):
    _StateChange = t.Tuple[_T_State, _T_State]
    state_changes: t.Mapping[_StateChange, t.Callable[..., t.Coroutine[t.Any, t.Any, None]]]
    starting_state: _T_State
    end_state: _T_State

    def __init_subclass__(cls, **kwargs):
        needed_attrs = ['state_changes', 'starting_state', 'end_state']
        missing = [attr for attr in needed_attrs if not hasattr(cls, attr)]
        if missing:
            raise NotImplementedError(f"{cls} needs to define class attribute[s] {', '.join(missing)}")

        if not isinstance(cls.state_changes, EnumMatchMapping):
            cls.state_changes = EnumMatchMapping(mapping=cls.state_changes)

    def __init__(self) -> None:
        super().__init__()
        self._state = self.starting_state
        self._run = None

    def _get_state(self) -> _T_State:
        return self._state

    def _set_state(self, new_state: _T_State | Exception):
        current = self._state
        if new_state == current:
            return

        # it is allowed to change the state if a matching callback is defined
        if not self.state_changes.get((current, new_state), None):
            raise ValueError(f"Can't change state {type(current)(current)!r} -> {type(new_state)(new_state)!r}")

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

    def peek_state(self) -> _T_State:
        """Look at the state without all the accessor shenanigans"""
        return self._state

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


class RunProtocol(util.wrapper.CoroutineWrapper):
    def __init__(self, protocol: UbiiProtocol):
        self._protocol = protocol
        super().__init__(coroutine=self._run())

    async def _no_callback_found(self, protocol, context):
        raise RuntimeError(f"No callback found for context {context} in {protocol}")

    async def _run(self):
        previous = None
        end_state = self._protocol.end_state
        current = self._protocol.starting_state
        context = types.SimpleNamespace()

        while previous != end_state:
            callback = self._protocol.state_changes.get((previous, current), self._no_callback_found)
            context.state_change = (previous, current)
            coro = callback(self._protocol, context)
            if not asyncio.iscoroutine(coro):
                raise RuntimeError(f"{callback} did not return a coroutine (not using `async def`?)")

            await coro
            previous = current
            if current != end_state:
                # wait until a state is set that is not the previous state
                current = await self._protocol.state.get(
                    predicate=lambda: self._protocol.peek_state() != previous
                )
