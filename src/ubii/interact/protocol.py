import logging

import enum
import types
import typing as t

from . import util


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


T_State = t.TypeVar('T_State', bound=enum.Enum)


class UbiiProtocol(t.Generic[T_State]):
    StateChange = t.Tuple[T_State, T_State]
    state_changes: t.Dict[StateChange, t.Callable[..., t.Coroutine[t.Any, t.Any, None]]]
    starting_state: T_State
    end_state: T_State

    def __init__(self: 'UbiiProtocol[T_State]') -> None:
        self._state = self.starting_state

    def _get_state(self: 'UbiiProtocol[T_State]') -> T_State:
        return self._state

    def _set_state(self: 'UbiiProtocol[T_State]', new_state: T_State):
        current = self._state
        if new_state == current:
            return

        if (current, new_state) not in self.state_changes:
            raise ValueError(f"Can't change state to {current} -> {new_state}")

        self._state = new_state

    # declaring the property this way helps pycharm to infer types correctly see
    # https://youtrack.jetbrains.com/issue/PY-15176 and related issues.
    state = util.condition_property(fget=_get_state, fset=_set_state)


class RunProtocol(util.CoroutineWrapper):
    def __init__(self, protocol: UbiiProtocol):
        self._protocol = protocol
        super().__init__(coroutine=self._run())

    async def _no_callback_found(self, context):
        raise RuntimeError(f"No callback found for context {context}")

    async def _run(self):
        previous = None
        end_state = self._protocol.end_state
        current = self._protocol.starting_state
        context = types.SimpleNamespace

        while previous != end_state:
            callback = self._protocol.state_changes.get((previous, current), self._no_callback_found)
            context.states = (previous, current)
            context = await callback(context)
            previous = current
            a = self._protocol.state.get()
