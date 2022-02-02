from __future__ import annotations

import enum
import typing as t
from typing import TypeVar, Type, Optional, Coroutine, Any, Callable, Tuple


try:
    from typing import runtime_checkable, Protocol
except ImportError:
    from typing_extensions import runtime_checkable, Protocol

T = TypeVar('T')
S = TypeVar('S')
R = TypeVar('R')

T_Exception = TypeVar('T_Exception', bound=Exception)
T_EnumFlag = TypeVar('T_EnumFlag', bound=enum.IntFlag)

T_co = TypeVar('T_co', covariant=True)
T_contra = TypeVar('T_contra', contravariant=True)

SimpleCoroutine = Coroutine[Any, Any, T]
Decorator = Callable[[Callable], Callable]

ExcInfo = Tuple[Optional[Type[T_Exception]], Optional[T_Exception], Any]


@runtime_checkable
class Descriptor(Protocol[T_co]):
    def __get__(self, instance: Any | None = ..., owner: Type[Any] | None = ...) -> T_co: ...

