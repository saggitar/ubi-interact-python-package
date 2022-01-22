from __future__ import annotations

import sys
from functools import lru_cache
if sys.version_info <= (3, 8):
    lru_cache = lru_cache(maxsize=None)

from proto.marshal import Marshal
from proto.marshal.rules.message import MessageRule

import ubii.proto as ub

__protobuf__ = ub.__protobuf__


class UbiiError(ub.Error, Exception, metaclass=ub.ProtoMeta):
    @classmethod
    @lru_cache
    def rule(cls):
        return MessageRule(ub.Error.pb(), cls)

    @property
    def args(self):
        return self.title, self.message, self.stack

    def __str__(self):
        values = {
            'type': self.__class__.__name__,
            'title': self.title,
            'message': self.message,
            'stack': self.stack
        }
        fmt = ', '.join(f"{k}=" + '{' + k + '}' for k, v in values.items() if v)
        return f"<{fmt.format(**values)}>"


class SessionRuntimeStopServiceError(UbiiError):
    pass


class ErrorRule(MessageRule):
    def to_python(self, value, *, absent: bool | None = None):
        title = value.title or ''
        if title.startswith('SessionRuntimeStopService'):
            return SessionRuntimeStopServiceError.rule().to_python(value, absent=absent)
        else:
            return UbiiError.rule().to_python(value, absent=absent)

    def to_proto(self, value):
        return super().to_proto(value)


Marshal(name=__protobuf__.marshal).register(ub.Error.pb(), ErrorRule(ub.Error.pb(), UbiiError))  # type: ignore


class RestartError(Exception):
    pass