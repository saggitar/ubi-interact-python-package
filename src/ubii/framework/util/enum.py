from __future__ import annotations

import typing

from itertools import chain

from .typing import T_EnumFlag, T


class EnumMatcher:
    """
    Used to compare tuples of :class:`~enum.IntFlag` enums. Tuples are considered 'matching' if
    all flags from the query enums are also set in the base enum.

    Example:
        Consider the enum ::

            >>> import enum
            >>> class Flags(enum.IntFlag):
            ...     ONE = enum.auto()
            ...     TWO = enum.auto()
            ...     COMBINED = ONE | TWO

        Because how :class:`~enum.IntFlag` enums work, we now have
        ``Flags.ONE == int(b'01') == 1`` and ``Flags.TWO == int(b'10') == 2``
        and ``Flags.COMBINED == int(b'11') == 3``. Since all flags in the ``COMBINED``
        value are set, we have

            >>> from ubii.framework.util import EnumMatcher
            >>> EnumMatcher.flags_match(base=Flags.ONE, query=Flags.COMBINED)
            True
            >>> EnumMatcher.flags_match(base=Flags.COMBINED, query=Flags.ONE)
            False

        And for tuples of values ::

            >>> values = (Flags.ONE, Flags.ONE, Flags.TWO, Flags.COMBINED)
            >>> EnumMatcher.matches(base=values, query=tuple([Flags.COMBINED] * len(values)))
            True

        If we have a mapping of :class:`~enum.IntFlag` tuples to some values, we can extract values for 'matching'
        keys:

            >>> mapping = {
            ...     (Flags.ONE, Flags.TWO): "Foo",
            ...     (Flags.TWO, Flags.COMBINED): "Bar"
            ... }
            >>> EnumMatcher.get_matching_value(query=(Flags.COMBINED, Flags.COMBINED), mapping=mapping)
            ["Foo", "Bar"]

    """
    _no_default = object()

    @classmethod
    def flags_match(cls, base: T_EnumFlag, query: T_EnumFlag):
        """
        Compute if all flags set in base are also set in query by computing ``base & query == query``
        """
        return base & query == query

    @classmethod
    def matches(cls, base: typing.Sequence[T_EnumFlag], query: typing.Sequence[T_EnumFlag]) -> bool:
        """

        Args:
            base: a tuple of flag enums that need to be matched
            query: a tuple of flag enums

        Returns:
            ``True`` if :meth:`.flags_match` is ``True`` at every position of the tuples.
            ``False`` otherwise or if tuples have different length or if they contain ``None`` values.
        """
        if not len(base) == len(query):
            return False

        if base == query:
            return True

        if any(x is None for x in chain(base, query)):
            return False

        return all(cls.flags_match(base_val, query_val) for base_val, query_val in zip(base, query))

    @classmethod
    def get_matching_value(cls,
                           query: typing.Sequence[T_EnumFlag],
                           default: typing.Any = _no_default,
                           *,
                           mapping: typing.Mapping[typing.Sequence[T_EnumFlag], T]) -> T:
        """
        Args:
            query: tuple used as query for :meth:`.matches`
            default: default value if no matches are found -- optional
            mapping: Mapping :math:`(IntFlag, IntFlag, ...) \\rightarrow Value`, keys are considered ``base`` values for
                :meth:`.matches`

        Returns:
            All values from ``mapping`` where :meth:`EnumMatcher.matches(key, query) <.matches>` is ``True``,
            otherwise ``default`` if set.

        Raises:
            KeyError: if no matches for ``query`` found in ``mapping`` and no ``default`` is given
        """

        matching = [value for enums, value in mapping.items() if cls.matches(enums, query)]
        if len(matching) != 1 and default is EnumMatcher._no_default:
            raise KeyError(f"Found matching values {matching} for query {query}, not exactly one match")

        return matching[0] if matching else default
