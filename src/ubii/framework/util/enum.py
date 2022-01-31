from __future__ import annotations

import typing as t
from itertools import chain

from .typing import T_EnumFlag, T


class EnumMatcher:
    _enum_tuple = t.Tuple[T_EnumFlag, ...]
    _no_default = object()

    @classmethod
    def matches(cls, base: _enum_tuple, query: _enum_tuple) -> bool:
        if not len(base) == len(query):
            return False

        if base == query:
            return True

        if any(x is None for x in chain(base, query)):
            return False

        return all(b & q == q for b, q in zip(base, query))

    @classmethod
    def get_matching_value(cls, key: _enum_tuple, default: t.Any = _no_default, *,
                           mapping: t.Mapping[_enum_tuple, T],
                           ) -> T:
        matching = [value for enums, value in mapping.items() if cls.matches(enums, key)]
        if len(matching) != 1 and default is EnumMatcher._no_default:
            raise KeyError(f"Found matching values {matching} for query {key}, not exactly one match")

        return matching[0] if matching else default
