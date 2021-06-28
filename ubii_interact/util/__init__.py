from typing import TypeVar, Mapping


def iterator(value):
    """
    Sometimes we need to have an iterator from something that can either be itself an iterator or a simple value
    :param value: iterator or value
    """
    try:
        yield from value
    except TypeError:
        yield value


class UbiiError(Exception):
    def __init__(self, title=None, message=None, stack=None):
        super().__init__(message)
        self.title = title
        self.message = message
        self.stack = stack


T = TypeVar('T')

class AliasDict(Mapping[str, T]):
    def __init__(self, data: Mapping[str, T], aliases=None) -> None:
        super().__init__()
        self.data = data
        self.aliases = aliases or {}

    def __contains__(self, item):
        """
        Since items can't be set to None, this check is ok.
        """
        return self[item] is not None

    def search(self, key):
        return self.data[self.aliases[key]] if key in self.aliases else self.data[key]

    def add_alias(self, key, alias):
        if alias in self.aliases:
            raise ValueError("Alias already exists.")

        self.aliases[alias] = key

    def with_aliases(self, aliases):
        for key, alias in aliases.items():
            self.add_alias(key, alias)

        return self

    def __getitem__(self, key):
        return self.search(key)

    def __setitem__(self, key, value):
        raise AttributeError("Values in AliasDicts are read only")

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return str(self.data)

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)