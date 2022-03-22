"""
Defines some special TypeVars and Type Aliases to be used throughout the codebase.

Also fixes the imports for some names that are not compatible with older python versions, namely
:obj:`.runtime_checkable` and :obj:`.Protocol`, so the special import handling can be done in one place
instead of every module that imports them.


Note:

    *   :obj:`.runtime_checkable` is an alias for :obj:`typing.runtime_checkable` or its backport from
        `typing_extensions`_ for older python interpreter versions

    *   :obj:`.Protocol` is an alias for :obj:`typing.Protocol` or its backport from `typing_extensions`_ for older
        python interpreter versions

    .. _typing_extensions: https://github.com/python/typing/blob/master/typing_extensions/README.rst
"""

from __future__ import annotations

import enum
from typing import (
    TypeVar,
    Coroutine,
    Any,
    Callable,
    Tuple,
    Optional,
    Type
)

try:
    from typing import runtime_checkable, Protocol
except ImportError:
    from typing_extensions import runtime_checkable, Protocol

T = TypeVar('T')
"""
Simple TypeVar
"""
S = TypeVar('S')
"""
Simple TypeVar
"""
R = TypeVar('R')
"""
Simple TypeVar
"""

T_Exception = TypeVar('T_Exception', bound=Exception)
"""
bound to :class:`Exception`
"""
T_EnumFlag = TypeVar('T_EnumFlag', bound=enum.IntFlag)
"""
bound to :class:`enum.IntFlag`
"""

T_co = TypeVar('T_co', covariant=True)
"""
covariant values
"""
T_contra = TypeVar('T_contra', contravariant=True)
"""
contravariant values
"""

SimpleCoroutine = Coroutine[Any, Any, T]
"""
TypeAlias for a Coroutine that only cares about return Type
"""
Decorator = Callable[[Callable], Callable]
"""
A Callable that decorates other Callables
"""
ExcInfo = Tuple[Optional[Type[T_Exception]], Optional[T_Exception], Any]
"""
Types according to return of :func:`sys.exc_info`
"""


@runtime_checkable
class Descriptor(Protocol[T_co]):
    def __get__(self, instance: Any | None = ..., owner: Type[Any] | None = ...) -> T_co:
        """
        Descriptors need to have this method. For more inforation see https://docs.python.org/3/howto/descriptor.html
        """


@runtime_checkable
class Documented(Protocol):
    """
    Attributes:
        __doc__: Documented objects need to have a ``__doc__`` attribute
    """
    __doc__: str


__all__ = (
    'SimpleCoroutine',
    'Decorator',
    'ExcInfo',
    'Descriptor',
    'Documented',
    'Protocol',
    'runtime_checkable',
    'T',
    'S',
    'R',
    'T_Exception',
    'T_EnumFlag',
    'T_co',
    'T_contra',
)
"""
names that should be imported from :mod:`ubii.util.typing` and not from other modules
"""
