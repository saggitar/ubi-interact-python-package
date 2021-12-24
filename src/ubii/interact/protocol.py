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


subscribe_regex = partialmethod(_handle_subscribe, as_regex=True, unsubscribe=False)
subscribe_topic = partialmethod(_handle_subscribe, as_regex=False, unsubscribe=False)
unsubscribe_regex = partialmethod(_handle_subscribe, as_regex=True, unsubscribe=True)
unsubscribe_topic = partialmethod(_handle_subscribe, as_regex=False, unsubscribe=True)

T_State = t.TypeVar('T_State', bound=enum.Enum)


class UbiiProtocol(t.Generic[T_State]):
    StateChange = t.Tuple[T_State, T_State]
    state_changes: t.Dict[StateChange, t.Callable[..., t.Coroutine[t.Any, t.Any, None]]]
    starting_state: T_State
    end_state: T_State

    def __init__(self: 'UbiiProtocol[T_State]'):
        self._state = self.starting_state

    @util.condition_property
    def state(self: 'UbiiProtocol[T_State]') -> T_State:
        return self._state

    @state.setter
    def state(self: 'UbiiProtocol[T_State]', new_state: T_State):
        current = self._state
        if (current, new_state) not in self.state_changes:
            raise ValueError(f"Can't change state to {current} -> {new_state}")

        self._state = new_state


class RunProtocol(util.CoroutineWrapper):
    def __init__(self, protocol: UbiiProtocol):
        self._protocol = protocol
        super().__init__(coroutine=self._run())

    async def _run(self):
        end_state = self._protocol.end_state
        previous = None
        current = self._protocol.starting_state
        context = types.SimpleNamespace

        while previous != end_state:
            callback = self._protocol.state_changes.get((previous, current))
            context = await callback(context)
            previous = current
            current = await self._protocol.state.get()  # noqa
