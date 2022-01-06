from __future__ import annotations

from collections import defaultdict

import typing as t
from difflib import SequenceMatcher
from functools import partial
from itertools import chain

from ubii.interact._typing import T_co, Decorator, T_EnumFlag


def similar(choices, item, cutoff=0.75):
    _similarity = lambda k: SequenceMatcher(None, k, item).ratio()  # noqa
    return list(sorted(filter(lambda item: _similarity(item) > cutoff, choices), key=_similarity, reverse=True))


class EnumMatchMapping(t.Mapping[t.Tuple[T_EnumFlag, ...], T_co]):
    def __init__(self: EnumMatchMapping[T_EnumFlag, T_co], mapping=t.Mapping[t.Tuple[T_EnumFlag, ...], T_co]):
        self.data: t.Mapping[t.Tuple[T_EnumFlag, ...], T_co] = mapping

    @staticmethod
    def match(base: t.Tuple[T_EnumFlag, ...], query: t.Tuple[T_EnumFlag, ...]):
        if not len(base) == len(query):
            return False

        if base == query:
            return True

        if any(x is None for x in chain(base, query)):
            return False

        return all(b & q == q for b, q in zip(base, query))

    def __getitem__(self, key: t.Tuple[T_EnumFlag, ...]) -> T_co:
        matching = [value for enums, value in self.data.items() if EnumMatchMapping.match(enums, key)]
        if len(matching) != 1:
            raise KeyError(f"Found matching values {matching} for query {key}, not exactly one match")

        return matching[0]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self):
        return iter(self.data)


class apply:
    """
    Proxy decorator to apply multiple decorators
    """
    decorators: t.Set[Decorator]

    def __init__(self, decorators: t.Set[Decorator] | None = None):
        self.decorators = set() if decorators is None else decorators

    def __call__(self, func: t.Callable):
        for dec in self.decorators:
            func = dec(func)

        return func


class decorator_property:
    _decorators_for_owner: t.Dict[t.Type[t.Any], t.Set[Decorator]] = defaultdict(set)

    def __init__(self, func, registry=None, decorators=None):
        self._registry = {} if registry is None else registry
        self.func = func
        self._applied = False
        self.name: str | None = None
        self._decorators = decorators

    @property
    def decorators(self):
        # decorator access might change the decorators
        return self._decorators_for_owner

    def register_decorator(self, owner, decorator):
        self._decorators_for_owner[owner].add(decorator)

    def cache_clear(self):
        """
        Apply decorators again at next access
        """
        self._applied = False

    def __set_name__(self, owner, name):
        self._registry[(owner, name)] = self
        self._decorators_for_owner[owner].update(self._decorators)
        del self._decorators
        self.name = name

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        if not self._applied:
            owner = owner or type(instance)
            self.func = apply(self.decorators[owner])(self.func)

        return partial(self.func, instance)


def register_for_decorator(registry: t.Mapping | None = None, decorators: t.Set[Decorator] | None = None):
    return partial(decorator_property, registry=registry, decorators=decorators)
