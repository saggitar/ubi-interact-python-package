from __future__ import annotations
import re
import types
from warnings import warn
import asyncio
import logging
from typing import Any, Dict, Type, TypeVar, Generic, List, Callable, Tuple, Union, Optional, Awaitable
from ubii_interact.util import as_iterator

log = logging.getLogger(__name__)

T = TypeVar('T')
C = Union['Signal.Connection[T, R]', Callable[[T], Awaitable]]

class Signal(Generic[T]):
    __types__: Dict[str, Type[Signal]] = {}
    _default_signature = object

    class Connection:
        def __init__(self, *callbacks):
            self._hash = hash(tuple(sorted(callbacks)))

        def __str__(self):
            hashrepr = str(self._hash)
            hashrepr = f"{hashrepr[:3]}...{hashrepr[-3:]}" if len(hashrepr) > 8 else hashrepr
            return f"Connection<{hashrepr}>"

        def __eq__(self, other):
            if not isinstance(other, type(self)):
                return NotImplemented

            return hash(self) == hash(other)

        def __repr__(self):
            return str(self)

        def __hash__(self):
            return self._hash

    class Slot(Generic[T]):
        def __init__(self):
            self._callback_dict: Dict[Signal.Connection, Tuple[Callable[[T], Awaitable[Any]], ...]] = {}

        @property
        def _callbacks(self) -> List[Callable[[T], Awaitable[Any]]]:
            return [n for nested in self._callback_dict.values() for n in nested]

        def connect(self, *callbacks: Callable[[T], Awaitable[Any]]):
            connection = Signal.Connection(*callbacks)
            if connection in self._callback_dict:
                raise ValueError("Callbacks are already connected")

            self._callback_dict[connection] = callbacks
            log.debug(f"Connected {connection} to {self}")
            return connection

        def disconnect(self, slot: Optional[C] = None):
            if not slot:
                self._callback_dict.clear()
                return

            connection = Signal.Connection(slot) if callable(slot) else slot
            if connection not in self._callback_dict:
                raise ValueError("Slot is not connected")

            del self._callback_dict[connection]
            log.debug(f"Disconnected {connection} from {self}")

        async def emit(self, *args: T) -> Tuple:
            callbacks = [call(*args) for call in self._callbacks]
            g = asyncio.gather(*callbacks)
            try:
                results = await g
            except:
                g.cancel()
                raise
            else:
                return results

        def __str__(self):
            return f"{self.__class__.__name__} -> {len(self._callbacks)} connections"

        def __repr__(self):
            return str(self)

    def __init__(self):
        self.slots: Dict[Any, Signal.Slot[T]] = {}

    def __class_getitem__(cls, item):
        kls = cls.__get_type(item)
        alias = super(Signal, kls).__class_getitem__(item)
        return alias

    @classmethod
    def __get_type(cls: Type[Signal], param: T) -> Type[Signal[T]]:
        def _repr(_type):
            return f"{_type.__module__}.{_type.__qualname__}" if hasattr(_type, '__qualname__') else repr(_type)

        name = f"{cls.__name__}[{_repr(param)}]"
        generic = super(Signal, cls).__class_getitem__(T)
        return cls.__types__.setdefault(name, types.new_class(_repr(cls), (generic,), {'signature': param}))

    def __init_subclass__(cls, /, signature=object, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._default_signature = signature

    def __getitem__(self, type_: T) -> Signal.Slot[T]:
        if not isinstance(type_, type):
            raise ValueError("Specify the overload by type, e.g. signal[str].connect(...)")

        return self.slots.setdefault(type_, Signal.Slot())

    async def emit(self, *args: T) -> Tuple:
        calls = [slot.emit(*args) for sig, slot in self.slots.items()
                 if all(isinstance(t, s) for t, s in zip(args, as_iterator(sig)))]
        if not calls:
            argrepr = re.sub('\n', '', f"({', '.join(str(arg) for arg in args)})")
            warn(f"No slots found for emmitted arguments {argrepr}, maybe you are using the wrong datatype for the slots?")

        g = asyncio.gather(*calls)
        try:
            results = await g
        except:
            g.cancel()
            raise
        else:
            return results

    def connect(self, *callbacks: Callable[[T], Awaitable]):
        default_slot = self.slots.setdefault(self._default_signature, Signal.Slot())
        return default_slot.connect(*callbacks)

    def disconnect(self, *callbacks: Callable[[T], Awaitable]):
        default_slot = self.slots[self._default_signature]
        default_slot.disconnect(*callbacks)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{self.__class__.__name__} -> {len(self.slots)} slot[s]"


class MultiEvent(asyncio.Event):
    def __init__(self, *events):
        super(MultiEvent, self).__init__()
        self.events = set(events)

    def add_event(self, event):
        self.events.add(event)

    def wait(self) -> bool:
        return all(asyncio.gather(e.wait() for e in self.events))

    def is_set(self) -> bool:
        return all(e.is_set() for e in self.events)