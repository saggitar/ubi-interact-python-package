from __future__ import annotations

import types
import typing
from warnings import warn
import asyncio
import logging
from typing import Any, Dict, Type, TypeVar, Generic, List, Callable, Tuple, Union, Optional, Awaitable

log = logging.getLogger(__name__)

T = TypeVar('T')
D = TypeVar('D')


class Signal(Generic[T]):
    __types__: Dict[str, Type[Signal]] = {}
    _default_signature = object

    class Connection:
        def __init__(self, *callbacks):
            self._hashes = [hash(c) for c in callbacks]

        def __str__(self):
            hashrepr = ', '.join(str(h) for h in self._hashes)
            hashrepr = f"{hashrepr[:3]}...{hashrepr[-3:]}" if len(hashrepr) > 8 else hashrepr
            return f"Connection<{hashrepr}>"

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

    class Slot(Generic[T]):
        def __init__(self):
            self._callback_dict: Dict[Signal.Connection, Tuple[Callable[[T], ...], ...]] = {}

        @property
        def _callbacks(self) -> List[Callable[[T], ...]]:
            yield from (n for nested in self._callback_dict.values() for n in nested)

        def connect(self, *callbacks: Callable[[T], ...]) -> Optional['Signal.Connection']:
            if not callbacks:
                return

            connection = Signal.Connection(*callbacks)
            if connection in self._callback_dict:
                raise ValueError(f"Callbacks {callbacks} are already connected")

            connected = [c for c in self._callbacks if c in callbacks]
            if connected:
                raise ValueError(f"Callback[s] {connected} are already connected")

            log.debug(f"Connected {callbacks} to {self}")
            self._callback_dict[connection] = callbacks
            return connection

        def disconnect(self, connection: Optional[Union[Callable[[T], ...], 'Signal.Connection']] = None):
            if not connection:
                self._callback_dict.clear()
                return

            if connection not in self._callback_dict:
                contained = [c for c in self._callback_dict if connection in c]
                if not contained:
                    raise ValueError(f"{connection} is not connected to {self}")
                else:
                    raise ValueError(f"{connection} is part of connection[s] {contained}, disconnect with a reference to the connection.")

            del self._callback_dict[connection]
            log.debug(f"Disconnected {connection if connection else 'all connections'} from {self}")

        async def emit(self, arg: T) -> Tuple:
            callbacks = [c(arg) if asyncio.iscoroutinefunction(c) else asyncio.get_running_loop().run_in_executor(None, c, arg) for c in self._callbacks]
            g = asyncio.gather(*callbacks)
            try:
                results = await g
            except Exception:
                g.cancel()
                raise
            else:
                return results

    def __init__(self, *callbacks: Callable[[T], Awaitable]):
        self.slots: Dict[Any, Signal.Slot[T]] = {}
        self.connect(*callbacks)

    def __class_getitem__(cls, item: D):
        kls = cls.__get_type(item)
        alias = super(Signal, kls).__class_getitem__(item)
        return alias

    @classmethod
    def __get_type(cls: Type[Signal], param: T) -> Type[Signal]:
        def _repr(_type):
            return f"{_type.__module__}.{_type.__qualname__}" if hasattr(_type, '__qualname__') else repr(_type)

        name = f"{cls.__name__}[{_repr(param)}]"
        generic = super(Signal, cls).__class_getitem__(T)
        return cls.__types__.setdefault(name, typing.cast(Type[Signal], types.new_class(_repr(cls), (generic,), {'signature': param})))

    def __init_subclass__(cls, /, signature=object, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._default_signature = signature

    def __getitem__(self, type_: T) -> Signal.Slot[T]:
        if not isinstance(type_, type):
            raise ValueError("Specify the overload by type, e.g. signal[str].connect(...)")

        return self.slots.setdefault(type_, Signal.Slot())

    async def emit(self, arg: T) -> Tuple:
        calls = [slot.emit(arg) for sig, slot in self.slots.items() if isinstance(arg, sig)]
        if not calls:
            warn(f"No slots found for emmitted argument {arg}, maybe you are using the wrong datatype for the slots?")

        g = asyncio.gather(*calls)
        try:
            results = await g
        except:
            g.cancel()
            raise
        else:
            return results

    def connect(self, *callbacks: Callable[[T], ...]):
        default_slot = self.slots.setdefault(self._default_signature, Signal.Slot())
        return default_slot.connect(*callbacks)

    def disconnect(self, *callbacks: Callable[[T], ...]):
        default_slot = self.slots[self._default_signature]
        default_slot.disconnect(*callbacks)
