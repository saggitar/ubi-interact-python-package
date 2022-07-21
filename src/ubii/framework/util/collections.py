from __future__ import annotations

import abc
import fnmatch
import typing
from operator import add

from .functools import hook, document_decorator
from .typing import T_co


def merge_dicts(base: typing.Dict, merge: typing.Dict, op: typing.Callable = add) -> typing.Dict:
    def merge_op(left, right):
        if isinstance(left, dict) and isinstance(right, dict):
            return merge_dicts(left, right, op=op)
        else:
            return op(left, right)

    return {**base, **merge, **{k: merge_op(base[k], merge[k]) for k in base if k in merge}}


class MatchMappingMixin(typing.Mapping[str, T_co], abc.ABC):
    """
    Mixin that implements glob pattern matches on mapping keys
    """

    def match_name(self, name) -> typing.Tuple[T_co, ...]:
        """
        Returns all values where ``name`` matches the keys of contained values interpreted as a glob pattern.

        Example:

            >>> import collections
            >>> class Container(collections.OrderedDict, MatchMappingMixin): pass
            >>> container = Container({"foo": 1, "foo*": 2, "bar": 3})
            >>> val = container.match_name('foo')
            >>> print(val)
            (1, 2)

        See Also:
            :mod:`fnmatch` -- details about glob patterns

        """
        return tuple(val for pattern, val in self.items() if fnmatch.fnmatch(name=name, pat=pattern))

    def match_pattern(self, pattern) -> typing.Tuple[T_co, ...]:
        """
        Returns all values where the keys of contained values match the glob ``pattern``.

        Example:

            >>> import collections
            >>> class Container(collections.OrderedDict, MatchMappingMixin): pass
            >>> container = MatchMappingMixin({"foo": 1, "foo*": 2, "bar": 3})
            >>> val = container.match_pattern('foo')
            >>> print(val)
            (1)

        See Also:
            :mod:`fnmatch` -- details about glob patterns

        """
        return tuple(top for topic_pattern, top in self.items() if fnmatch.fnmatch(name=topic_pattern, pat=pattern))


T_Key = typing.TypeVar('T_Key')


class DefaultHookMap(typing.Generic[T_Key, T_co], typing.Mapping[T_Key, T_co]):
    """
    Acts like a :class:`~collections.defaultdict` mapping but it's :attr:`.default_factory` is always a
    :class:`util.hook` so it can be adjusted after creation of the mapping
    """

    def __init__(self: DefaultHookMap[T_Key, T_co], base_factory: typing.Callable[[T_Key], T_co]):
        """
        Args:
            base_factory: Saved as :attr:`.base_factory`
        """
        self.base_factory: typing.Callable[[T_Key], T_co] = base_factory
        """
        The base factory callable, gets used in :func:`.default_factory` to actually create the item
        """
        self.data: typing.Dict[str, T_co] = {}

    @hook
    @document_decorator(hook)
    def default_factory(self, key: T_Key) -> None:
        """
        Called whenever a key is not present in the mapping.
        Creates new value using the :attr:`.base_factory`

        Example:

            >>> from ubii.framework.util import DefaultHookMap
            >>> class Topic:
            ...     def __init__(self, pattern):
            ...             self.pattern = pattern
            ...
            >>> store = DefaultHookMap(default_factory=Topic)
            >>> topic = store['topic/glob/pattern']
            >>> assert topic.pattern == 'topic/glob/pattern'

        Args:
            key: glob pattern

        """
        self.data[key] = self.base_factory(key)

    def __getitem__(self, key: T_Key) -> T_co:
        if key not in self.data:
            self.default_factory(key)
        assert key in self.data
        return self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> typing.Iterator[T_Key]:
        return iter(self.data)

    def __contains__(self, item: T_Key):
        return item in self.data

    def __delitem__(self, key: T_Key):
        if key not in self.data:
            raise KeyError(f"Can't delete item with key {key}")

        del self.data[key]
