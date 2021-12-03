from functools import lru_cache

from proto.marshal import Marshal
from proto.marshal.rules.message import MessageRule

import ubii.proto
from ubii.proto import Error, ProtoMeta

__protobuf__ = ubii.proto.__protobuf__


class UbiiError(Error, Exception, metaclass=ProtoMeta):
    @classmethod
    @lru_cache
    def rule(cls):
        return MessageRule(Error.pb(), cls)

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
    def to_python(self, value, *, absent: bool = None):
        title = value.title or ''
        if title.startswith('SessionRuntimeStopService'):
            return SessionRuntimeStopServiceError.rule().to_python(value, absent=absent)

        return super().to_python(value, absent=absent)

    def to_proto(self, value):
        return super().to_proto(value)


Marshal(name=__protobuf__.marshal).register(Error.pb(), ErrorRule(Error.pb(), UbiiError))  # type: ignore
