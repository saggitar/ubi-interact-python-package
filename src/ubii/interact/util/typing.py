from __future__ import annotations

import enum
import typing as t

_T = t.TypeVar('_T')
_S = t.TypeVar('_S')
_R = t.TypeVar('_R')

_T_State = t.TypeVar('_T_State', bound=enum.IntFlag)
_T_Exception = t.TypeVar('_T_Exception', bound=Exception)

_T_co = t.TypeVar('_T_co', covariant=True)
_T_contra = t.TypeVar('_T_contra', contravariant=True)


_T_Callable = t.TypeVar('_T_Callable', bound=t.Callable)
_SimpleCoroutine = t.Coroutine[t.Any, t.Any, _T]
_Decorator = t.Callable[[t.Callable], t.Callable]

_ExcInfo = t.Tuple[t.Optional[t.Type[_T_Exception]], t.Optional[_T_Exception], t.Any]


@t.runtime_checkable
class _Descriptor(t.Protocol[_T_co]):
    def __get__(self, instance: t.Any | None = None, owner: t.Type[t.Any] | None = None) -> _T_co: ...


_T_EnumFlag = t.TypeVar('_T_EnumFlag', bound=enum.IntFlag)
