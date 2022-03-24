"""
To get specific errors to raise for the different errors the `master node` can send during communication
with a `client node` this module defines some new Exception types that also inherit from :class:`ubii.proto.Error`
and rules for the :class:`proto.marshal.Marshal` for serialization of these Exceptions on the fly.

Importing this module registers the rules for the marshal of the :mod:`ubii.proto` module to convert between
the `Error` protobuf messages (internally used by the :class:`ubii.proto.Error` wrapper) and the new :class:`UbiiError`
exception type i.e. when this module is imported `Error` protobuf messages will not be converted to plain
:class:`ubii.proto.Error` wrapper objects, but instead to :class:`UbiiError` objects so that they can be used in a
:ref:`raise` expression. Depending on e.g. the value if the :attr:`ubii.proto.Error.title` subtypes of
:class:`UbiiError` might be used, i.e. it's possible to differentiate the type of error in an :ref:`except` statement.

Example:

    A short overview of the :doc:`proto-plus marshaling process <plus:marshal>` (since it's not documented in detail):

        Get the marshal name used in your `proto-plus` module, in this case ::

            import ubii.proto
            marshal_name = ubii.proto.__protobuf__.marshal

        Get a reference to the marshal by name (same name returns same object, see documentation of
        :class:`proto.marshal.Marshal`) ::

            import proto.marshal
            marshal = proto.marshal.Marshal(name=marshal_name)

        Create a rule -- you can use the existing rules of the `proto-plus` package as a starting point ::

            from proto.marshal.rules.message import MessageRule
            class CustomRule(MessageRule):
                def to_proto(self, value):
                    # convert wrapper to actual protobuf message type and return it
                    ...

                def to_python(self, value, *, absent: bool | None = None):
                    # convert actual protobuf message to python wrapper and return it,
                    ...

        Register the rule at the Marshal you retrieved earlier. To do that you need the actual protobuf type
        that you want to handle with the rule, if you only got `proto-plus` wrappers at your disposal, you need
        to first get the internal type from the wrapper ::

            Error_pb = ubii.proto.Error.pb()  # actual protobuf type created for the Error wrapper
            marshal.register(Error_pb, CustomRule(descriptor=Error_pb, wrapper=ubii.proto.Error))


See Also:
    :class:`proto.marshal.Marshal` -- documentation of the Marshal class in the `proto-plus` module.

    :obj:`ubii.proto.__protobuf__` -- module level variable of the `ubii-msg-formats` module to expose the `proto-plus`
    marshal and message pool
"""

from __future__ import annotations

import sys
from functools import lru_cache

from ubii.framework import util

if sys.version_info <= (3, 8):
    lru_cache = lru_cache(maxsize=None)

from proto.marshal import Marshal
from proto.marshal.rules.message import MessageRule

import ubii.proto

__protobuf__ = ubii.proto.__protobuf__


@util.dunder.repr('title', 'message', 'stack')
class UbiiError(ubii.proto.Error, Exception, metaclass=ubii.proto.ProtoMeta):
    """
    Base class for all custom errors.
    """

    @classmethod
    @lru_cache
    def rule(cls) -> MessageRule:
        """
        Fallback rule to convert between :class:`ubii.proto.Error` and this type
        """
        return MessageRule(ubii.proto.Error.pb(), cls)

    @property
    def args(self):
        """
        Tuple of (:attr:`.title`, :attr:`.message`, :attr:`.stack`)
        """
        return self.title, self.message, self.stack


class SessionRuntimeStopServiceError(UbiiError):
    """
    Raise if a service request to stop a :class:`ubii.proto.Session` is unsuccessful.
    """


class ErrorRule(MessageRule):
    """
    Custom `MessageRule` to convert `Error` protobuf messages to :class:`UbiiError`
    or :class:`SessionRuntimeStopServiceError` exceptions.
    """

    def to_python(self, value, *, absent: bool | None = None):
        """
        If the :attr:`ubii.proto.Error.title` starts with ``'SessionRuntimeStopService'`` the message is converted to
        a :class:`SessionRuntimeStopServiceError`, otherwise a normal :class:`UbiiError` is created.
        """
        title = value.title or ''
        if title.startswith('SessionRuntimeStopService'):
            return SessionRuntimeStopServiceError.rule().to_python(value, absent=absent)
        else:
            return UbiiError.rule().to_python(value, absent=absent)

    def to_proto(self, value):
        """
        Converts back to a protobuf message using the `MessageRule` implementation from the `proto-plus` package
        """
        return super().to_proto(value)


Marshal(name=__protobuf__.marshal).register(
    ubii.proto.Error.pb(),  # type: ignore
    ErrorRule(ubii.proto.Error.pb(), UbiiError)  # type: ignore
)
