"""
We need to store subscribers and topics nicely to efficiently find them and notify the subscribers.
Therefore we use the `anytree` library to generate a topic tree. Each topic has slots for callbacks
with different argument types (default TopicDataRecord).

"""

from __future__ import annotations
from warnings import warn
import asyncio
import logging
from typing import Any, Dict, TypeVar, List, Callable, Tuple, Union, Optional, Awaitable

from ..util import TypedGeneric

log = logging.getLogger(__name__)

T = TypeVar('T')

class Topic(TypedGeneric[T]):
    class Token:
        def __init__(self, *callbacks):
            self._hashes = [hash(c) for c in callbacks]

        def __str__(self):
            hashrepr = ', '.join(str(h) for h in self._hashes)
            hashrepr = f"{hashrepr[:3]}...{hashrepr[-3:]}" if len(hashrepr) > 8 else hashrepr
            return f"Token<{hashrepr}>"

        def __eq__(self, other):
            if not isinstance(other, type(self)):
                return NotImplemented

            return hash(self) == hash(other)

        def __repr__(self):
            return str(self)

        def __hash__(self):
            return hash(tuple(sorted(self._hashes)))

        def __contains__(self, item):
            if not callable(item):
                return NotImplemented

            return hash(item) in self._hashes

    class Slot(TypedGeneric[T]):
        def __init__(self):
            self._callback_dict: Dict[Topic.Token, Tuple[Callable[[T], ...], ...]] = {}

        @property
        def _callbacks(self) -> List[Callable[[T], ...]]:
            """
            Flat iterator for all connected callbacks
            """
            yield from (n for nested in self._callback_dict.values() for n in nested)

        def connect(self, *callbacks: Callable[[T], ...]) -> Optional['Topic.Token']:
            if not callbacks:
                return

            connection = Topic.Token(*callbacks)
            if connection in self._callback_dict:
                raise ValueError(f"Callback[s] {callbacks} already connected")

            connected = [c for c in self._callbacks if c in callbacks]
            if connected:
                raise ValueError(f"Callback[s] {connected} already connected")

            log.debug(f"Connected {callbacks} to {self}")
            self._callback_dict[connection] = callbacks
            return connection

        def disconnect(self, token: Optional[Union[Callable[[T], ...], 'Topic.Token']] = None):
            if not token:
                self._callback_dict.clear()
                return

            if token not in self._callback_dict:
                contained = [c for c in self._callback_dict if token in c]
                if not contained:
                    raise ValueError(f"{token} is not connected to {self}")
                else:
                    raise ValueError(
                        f"{token} is part of connection[s] {contained}, disconnect with a reference to the connection.")

            del self._callback_dict[token]
            log.debug(f"Disconnected {token if token else 'all connections'} from {self}")

        async def emit(self, arg: T) -> Tuple:
            callbacks = [c(arg) if asyncio.iscoroutinefunction(c)
                         else asyncio.get_running_loop().run_in_executor(None, c, arg)
                         for c in self._callbacks]

            g = asyncio.gather(*callbacks)
            try:
                results = await g
            except Exception:
                g.cancel()
                raise
            else:
                return results

    def __init__(self, *callbacks: Callable[[T], Awaitable]):
        self.slots: Dict[Any, Topic.Slot[T]] = {}
        self.connect(*callbacks)

    async def emit(self, arg: T) -> Tuple:
        calls = [slot.emit(arg) for sig, slot in self.slots.items() if isinstance(arg, sig)]
        if not calls:
            warn(f"No slots found for emitted argument {arg}, maybe you are using the wrong datatype for the slots?")

        g = asyncio.gather(*calls)
        try:
            results = await g
        except:
            g.cancel()
            raise
        else:
            return results

    def connect(self, *callbacks: Callable[[T], ...]):
        default_slot = self.slots.setdefault(self._signature, self.Slot())
        return default_slot.connect(*callbacks)

    def disconnect(self, *callbacks: Callable[[T], ...]):
        default_slot = self.slots[self._signature]
        default_slot.disconnect(*callbacks)
