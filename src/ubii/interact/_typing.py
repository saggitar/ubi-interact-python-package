from __future__ import annotations

import enum
import typing as t

T = t.TypeVar('T')
S = t.TypeVar('S')
R = t.TypeVar('R')

T_State = t.TypeVar('T_State', bound=enum.IntFlag)
T_Exception = t.TypeVar('T_Exception', bound=Exception)

T_co = t.TypeVar('T_co', covariant=True)
T_contra = t.TypeVar('T_contra', contravariant=True)

SimpleCoroutine = t.Coroutine[t.Any, t.Any, T]
Decorator = t.Callable[[t.Callable], t.Callable]

ExcInfo = t.Tuple[t.Optional[t.Type[T_Exception]], t.Optional[T_Exception], t.Any]


class Descriptor(t.Protocol[T_co]):
    def __get__(self, instance: t.Any | None = None, owner: t.Type[t.Any] | None = None) -> T_co: ...


T_EnumFlag = t.TypeVar('T_EnumFlag', bound=enum.IntFlag)
