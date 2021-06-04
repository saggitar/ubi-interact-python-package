import re

from warnings import warn

import asyncio
import logging

from typing import Any, Dict

from ubii_interact.util import apply, as_iterator
from ubii_interact.util.translators import ProtoMessages

log = logging.getLogger(__name__)

class MetaSignal(type):
    __signal_types__ = {}

    def __new__(mcs, name, bases, dct, **kwargs) -> Any:
        dct['__getitem__'] = mcs.__getitem__
        return super().__new__(mcs, name, bases, dct)

    def __getitem__(self, item) -> type:
        return MetaSignal.__signal_types__.setdefault(item, type(f"Signal[{item.__name__}]", (self,), {'_default_slot_signature': item}))


class Signal(object, metaclass=MetaSignal):
    _default_slot_signature = object

    class _metaslot(type):
        __slottypes__ = {}

        def __new__(mcs, name, bases, dct, **kwargs) -> Any:
            dct['__getitem__'] = mcs.__getitem__
            return super().__new__(mcs, name, bases, dct)

        def __getitem__(self, item) -> type:
            klass = type(f"Slot[{item.__name__}]", (self,), {'signature': item})
            return Signal._metaslot.__slottypes__.setdefault(item, klass)

    class Connection:
        def __init__(self, *callbacks):
            self._hash = hash(callbacks)

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

    class Slot(metaclass=_metaslot):
        _signature = object

        def __init_subclass__(cls, /, **kwargs):
            cls._signature = kwargs.pop('signature', None) or cls._signature
            super().__init_subclass__(**kwargs)

        def __init__(self):
            self._callback_dict = {}

        @property
        def _callbacks(self):
            return [n for nested in self._callback_dict.values() for n in nested]

        def connect(self, *callbacks):
            connection = Signal.Connection(*callbacks)
            if connection in self._callback_dict:
                raise ValueError("Callbacks are already connected")

            self._callback_dict[connection] = callbacks
            log.debug(f"Connected {connection} to {self}")
            return connection

        def disconnect(self, slot=None):
            if not slot:
                self._callback_dict.clear()
                return

            connection = Signal.Connection(slot) if callable(slot) else slot
            if not connection in self._callback_dict:
                raise ValueError("Slot is not connected")

            del self._callback_dict[connection]
            log.debug(f"Disconnected {connection} from {self}")

        async def emit(self, *args, **kwargs):
            callbacks = [call(*args, **kwargs) for call in self._callbacks]
            return await asyncio.gather(*callbacks)

        def __str__(self):
            return f"{self.__class__.__name__} -> {len(self._callbacks)} connections"

        def __repr__(self):
            return str(self)


    def __init_subclass__(cls, **kwargs):
        cls._default_slot_signature = kwargs.pop('_default_slot_signature', None) or cls._default_slot_signature
        super().__init_subclass__(**kwargs)

    def __init__(self):
        self._slots: Dict[Any, Signal.Slot] = {}

    def __getitem__(self, type_) -> 'Signal.Slot':
        if not isinstance(type_, type):
            raise ValueError("Specify the overload by type, e.g. signal[str].connect(...)")

        return self._slots.setdefault(type_, Signal.Slot[type_]())

    async def emit(self, *args):
        calls = [slot.emit(*args) for sig, slot in self._slots.items()
                 if all(isinstance(t, s) for t, s in zip(args, as_iterator(sig)))]
        if not calls:
            argrepr = re.sub('\n', '', f"({', '.join(str(arg) for arg in args)})")
            warn(f"No slots found for emmitted arguments {argrepr}, maybe you are using the wrong datatype for the slots?")

        return await asyncio.gather(*calls)

    def connect(self, *slots):
        default_slot = self._slots.setdefault(self._default_slot_signature, Signal.Slot[self._default_slot_signature]())
        return default_slot.connect(*slots)

    def disconnect(self, *slots):
        default_slot = self._slots[self._default_slot_signature]
        default_slot.disconnect(*slots)


ProtoSignals: Dict[str, type] = apply(lambda t: Signal[t.proto] if t else None, ProtoMessages)
