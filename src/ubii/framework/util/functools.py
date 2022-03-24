from __future__ import annotations

import asyncio
import difflib
import functools
import inspect
import logging
import pickle
import re
import sys
import typing
import warnings
from collections import namedtuple

import ubii.proto as ub
from . import RegistryMeta
from .typing import S, T, ExcInfo


def similar(choices: typing.Sequence, item: typing.Any, cutoff=0.70):
    """
    Use this e.g. if you think your users can't type <:

    Example:

        >>> >>> from ubii.framework import util
        >>> choices = ["Foo", "Bar", "Foobar", "Thing"]
        >>> util.similar(choices, "Thong")
        ['Thing']
        >>> util.similar(choices, "foo")
        []
        >>> util.similar(choices, "foo", cutoff=0.5)
        ['Foo']
        >>> util.similar(choices, "foo", cutoff=0.4)
        ['Foo', 'Foobar']


    Args:
        choices: sequence of items
        item: something not in ``choices`` but possibly 'similar'
        cutoff: threshold for similarity measure

    Returns:
        elements from ``choices`` with :math:`similarity(choice, item) > cutoff`

    See Also:
        :class:`difflib.SequenceMatcher` -- used internally to compute similarity score
    """
    _similarity = lambda k: difflib.SequenceMatcher(None, k, item).ratio()  # noqa
    return list(sorted(filter(lambda item: _similarity(item) > cutoff, choices), key=_similarity, reverse=True))


T_Callable = typing.TypeVar('T_Callable', bound=typing.Callable[..., typing.Any])


class _append_doc:
    """
    Helper to append information to docstrings of callables
    """
    _whitespace = re.compile(r'^( +)[^\s]+', re.MULTILINE)

    def __init__(self, cb):
        self.cb = cb

    def __call__(self, info: str):
        doc = self.cb.__doc__ or ''
        orig_indents = self._whitespace.findall(doc) or ['']
        info_indents = self._whitespace.findall(info) or ['']

        # check if indents are consistent:
        assert all(
            indent.startswith(orig_indents[0]) for indent in orig_indents
        ), f"Inconsistent indents for {self.cb}.__doc__"
        assert all(
            indent.startswith(info_indents[0]) for indent in info_indents
        ), f"Inconsistent indents for {info}"

        replace = functools.partial(re.compile(f"^{info_indents[0]}", re.MULTILINE).sub, orig_indents[0])
        self.cb.__doc__ = doc + '\n'.join(map(replace, info.split('\n')))


def document_decorator(decorator):
    def inner(decorated):
        # if not os.environ.get('SPHINX_DOC_BUILDING'):
        #     return decorated
        is_async = asyncio.iscoroutinefunction(decorated)
        sig = inspect.signature(decorated)

        if is_async:
            ret_type = '[' + sig.return_annotation.strip("'") + ']' if sig.return_annotation != sig.empty else ''
            decorated.__annotations__['return'] = f"Awaitable{ret_type}"

        info = {
            'decorator':
                f"{decorator.__module__}.{decorator.__qualname__}" if not isinstance(decorator, str) else decorator,
            'signature':
                "{}def {}{}".format("async " if is_async else '', decorated.__name__, sig)
        }

        _append_doc(decorated)(
            """
            This callable had the :obj:`~{decorator}` decorator applied.
            Original signature: ``{signature}``
            """.format(**info)
        )
        return decorated

    return inner


class calc_delta:
    """
    Calculate difference between values produced by a factory function

    Example:

        >>> from ubii.framework import util
        >>> value_factory = iter([1, 2, 70, 3, 4, 5]).__next__
        >>> delta = util.calc_delta(value_factory)
        >>> delta()
        1
        >>> delta()
        68
        >>> delta()
        -67
        >>> delta.value
        3

    """

    def __init__(self, get_value):
        self.get_value = get_value
        self.value = get_value()

    def __call__(self):
        previous = self.value
        self.value = self.get_value()
        return self.value - previous


class hook(typing.Generic[T_Callable]):
    """
    This decorator gives the decorated callable the ability to
    :meth:`register decorators <.register_decorator>`, i.e. define a consistent API to apply
    decorators to the decorated callable.

    Notes:

        *   Decorators need not to be unique, registering the same decorator multiple times will apply it multiple times.
        *   Decorators are applied at the first call to the :class:`hook`, i.e. the first call could take longer.
        *   :attr:`hook.func` is the unmodified callable passed during initialization. If necessary one can clear the
            cached 'applied' version of the callable with :meth:`hook.cache_clear`

    Warning:
        Decorator order depends on order of registration. The last registered decorator is applied last.
        We have :math:`hook = d_n \\circ d_{n-1} \\circ ... \\circ d_1 \\circ d_0 \\circ f` where
        :math:`0` to :math:`n` are the indices of the decorator in :attr:`hook.decorators`


    Example:

        You have some callable which is predestined to be slightly altered later. Instead of monkey patching it later,
        you can preemptively define that callable to be 'alterable' by converting it into a `hook`.

        >>> from ubii.framework import util
        >>> value_factory = iter([1, 2, 70, 3, 4, 5]).__next__
        >>> hook = util.hook(value_factory)
        >>> hook()
        1
        >>> hook()
        2
        >>> hook()
        70
        >>> def decorator(func):
        ...     def inner():
        ...             return -1 * func()
        ...     return inner
        ...
        >>> hook.register_decorator(decorator)
        >>> hook()
        -3
        >>> hook()
        -4

    """
    __name__: str

    def __init__(self: hook[T_Callable], func: T_Callable, decorators=None):
        """
        Create a hook from ``func``

        Args:
            func: a callable which should be more easily decorate-able
            decorators: initial decorators -- optional
        """
        self.func = func
        """
        Reference to unmodified callable
        """
        self._decorators = list(decorators or ())
        self._applied = None
        functools.wraps(func)(self)

    @property
    def decorators(self):
        """
        List of registered decorators
        """
        return self._decorators

    def register_decorator(self, decorator) -> None:
        """
        Add decorator to internal list of decorators and re-apply all decorators at next call

        Args:
            decorator: some callable
        """
        self._decorators.append(decorator)
        self.cache_clear()

    def cache_clear(self):
        """
        Apply decorators again at next access
        """
        self._applied = None

    def __call__(self, *args, **kwargs):
        if self._applied is None:
            self._applied = compose(*self.decorators)(self.func)

        return self._applied(*args, **kwargs)

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return functools.partial(self, instance)


class registry(typing.Generic[S, T]):
    """
    Decorator to register every call to another callable

    Example:

        Let's say we have another decorator that does some simple task

            >>> def my_decorator(func):
            ...     def inner(*args):
            ...         print("Foo")
            ...         return func(*args)
            ...     return inner

        We want to create a new decorator that does the same but keeps track of every
        decorated function

            >>> from ubii.framework import util
            >>> my_decorator_registry = util.registry(key=lambda func: func.__name__, fn=my_decorator)
            >>> @my_decorator_registry
            ... def test_function(foo: str):
            ...     print(foo)
            ...
            >>> my_decorator_registry.registry
            {'test_function': <function test_function at (...)>}
            >>> test_function("Bar")
            Foo
            Bar

    """

    def __init__(self: registry[S, T], key: typing.Callable[[T], S], fn: typing.Callable[..., T]):
        """
        This callable wraps the callable passed as `fn`, but caches results in :attr:`.registry`
        with `key(result)` as key.

        Args:
            key: computes some hashable unique value for possible results of wrapped callable
            fn: if this callable returns non-unique results, old cached values are technically overwritten since the
                ``key`` returns the same value for the same input by definition
        """
        self.key = key
        """
        computes keys for results of :attr:`.fn` to cache them inside :attr:`.registry`
        """
        self.fn = fn
        """
        wrapped callable
        """
        self._registry: typing.Dict[S, T] = {}
        functools.wraps(fn)(self)

    @property
    def registry(self) -> typing.Dict[S, T]:
        """
        Mapping :math:`key \\rightarrow result` from computed :attr:`key(result) <.key>` of all calls
        """
        return self._registry

    def __call__(self, *args, **kwargs):
        instance = self.fn(*args, **kwargs)
        self._registry[self.key(instance)] = instance
        return instance

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return functools.partial(self, instance)


def exc_handler_decorator(handler: typing.Callable[[ExcInfo], typing.Awaitable[None | bool] | None | bool]):
    """
    This callable takes an 'exception handler' i.e. a callable that processes results of :func:`sys.exc_info`
    and converts it to a decorator that can be itself applied to ``async`` callables to handle their exceptions.

    Example:

        >>> def handler(*exc_info):
        ...     if exc_info:
        ...             print(f"Got exception {exc_info[1]}")

        >>> from ubii.framework import util
        >>> handler_deco = util.exc_handler_decorator(handler)
        >>> @handler_deco
        ... async def foo(value):
        ...     print(5 // value)

        >>> async def main():
        ...     for value in [0,1,2,0,-2]:
        ...             await foo(value)

        >>> import asyncio
        >>> asyncio.run(main())
        Got exception integer division or modulo by zero
        5
        2
        Got exception integer division or modulo by zero
        -3

    Args:
        handler: an exception handler callable

    Returns:
        decorator to catch exceptions from ``async`` methods

    """

    def decorator(fun):
        @functools.wraps(fun)
        async def _inner(*args, **kwargs):
            try:
                result = fun(*args, **kwargs)
                if asyncio.isfuture(result) or asyncio.iscoroutine(result):
                    result = await result
                return result
            except:  # noqa
                exception_info = sys.exc_info()
                result = handler(*exception_info)
                if asyncio.isfuture(result) or asyncio.iscoroutine(result):
                    await result

        return _inner

    return decorator


def log_call(logger: logging.Logger):
    """
    Args:
        logger: calls are logged as :meth:`logging.Logger.debug`

    Returns:
        A decorator to log calls to decorated callable to ``logger``
    """

    def decorator(fun):
        @functools.wraps(fun)
        def __inner(*args):
            logger.debug(f"called {fun}")
            return fun(*args)

        return __inner

    return decorator


class AbstractAnnotations:
    attr_name = '__required_annotations__'
    wrap_marker = object()

    def __init__(self, *names):
        self.names = names

    def __call__(self, cls: type):
        if any(name not in cls.__annotations__ for name in self.names):
            raise ValueError(f"{cls} needs to have annotation[s] for {', '.join(self.names)}")

        names = getattr(cls, self.attr_name, set())
        names.update(self.names)
        setattr(cls, self.attr_name, names)

        original_new = cls.__new__

        wrapped = self.wrap_once(original_new)  # might return original_new if already decorated
        setattr(cls, original_new.__name__, wrapped)

        return cls

    def wrap_once(self, original_new):
        wrap_markers = getattr(original_new, 'markers', set())
        if self.wrap_marker in wrap_markers:
            return original_new

        @functools.wraps(original_new)
        def wrapped(cls, *args, **kwargs):
            instance = original_new(cls, *args, **kwargs)

            # is instance can be created type(instance) can't have abstract methods, so
            # instance also has to have all required attributes
            missing = [name for name in getattr(cls, self.attr_name, []) if not hasattr(cls, name)]
            if missing:
                raise TypeError(
                    f"Can't create {cls} instance with missing class attribute[s] {', '.join(map(repr, missing))}")

            return instance

        wrap_markers.add(self.wrap_marker)
        wrapped.markers = wrap_markers
        return wrapped


class ProtoRegistry(ub.ProtoMeta, RegistryMeta):
    """
    Instances for types that have this metaclass are registered, and can be serialized / deserialized
    according to their protobuf specifications.

    See Also:
        :class:`~codestare.async_utils.helpers.RegistryMeta` -- the :class:`ProtoRegistry` simply adds the serialization
            to the mechanisms for registration of instances inherited from here
    """

    def __new__(mcs, *args, **kwargs):
        kls = super().__new__(mcs, *args, **kwargs)
        return kls

    def _serialize_all(cls):
        return {key: cls.serialize(obj) for key, obj in cls.registry.items()}

    def _deserialize_all(cls, mapping: typing.Mapping):
        return {key: cls.deserialize(obj) for key, obj in mapping.items()}

    def save_specs(cls, path):
        """
        Serialize all registered Protocol Buffer Wrapper objects and pickle the results to ``path``
        """
        with open(path, 'wb') as file:
            pickle.dump(cls._serialize_all(), file)

    def update_specs(cls, path) -> None:
        """
        Loads specs from pickled file, updates all registered instances according to their specification
        from the pickled messages.

        Args:
            path: load binary pickle file from here

        """
        with open(path, 'rb') as file:
            loaded = pickle.load(file)

        specs = cls._deserialize_all(loaded)
        for key, item in cls.registry.items():
            spec = specs.get(key)
            if not spec:
                warnings.warn(f"No {cls} instance for key {key} registered, can't update")
                continue

            cls.copy_from(item, spec)


class function_chain:
    """
    Generates a callable that calls multiple functions in a defined order with same arguments

    Example:

        >>> def foo(value):
        ...     print(f"foo: {value}")
        >>> def bar(value):
        ...     print(f"bar: {value}")
        >>> from ubii.framework import util
        >>> chain = util.function_chain(foo, bar)
        >>> chain(1)
        foo: 1
        bar: 1

    See Also:
        :class:`compose` -- if you want to compose functions instead of passing the same arguments to each

        :class:`async_compose` -- if you want to compose coroutines
    """

    def __init__(self, *funcs):
        self.funcs = funcs
        """
        Tuple of functions that need to be called
        """

    def __call__(self, *args):
        for f in self.funcs:
            f(*args)

    def __get__(self, instance=None, owner=None):
        if instance is None:
            return self

        return functools.partial(self, instance)

    @classmethod
    def reverse(cls, *funcs):
        return cls(*reversed(funcs))


class compose:
    """
    Generates a callable that is the composition of other callables.
    Callables are called in the order in which they are passed to :class:`compose` i.e.
    :math:`compose(f, g) = g \\circ f`

    Example:
        >>> from ubii.framework import util
        >>> def foo(value):
        ...     return value + 1
        ...
        >>> def bar(value):
        ...     print(value)
        ...
        >>> foobar = util.compose(foo, bar)  # foobar(value) = bar(foo(value))
        >>> foobar(1)
        2

    See Also:
        :class:`function_chain` -- if you want to pass the same argument to every function instead of composing

        :class:`async_compose` -- if you want to compose coroutines

    """

    def __init__(self, *funcs):
        self._info = ', '.join(map(repr, funcs))
        self.funcs = funcs
        """
        Tuple of original callables
        """
        self._reduced = functools.reduce(lambda g, f: lambda *a: f(g(*a)), funcs) if funcs else (lambda x: x)

    def __call__(self, *args):
        return self._reduced(*args)

    def __repr__(self):
        return f"compose({self._info})"


class awaitable_predicate:
    """
    Typically, to let an ``async`` coroutine wait until some predicate is `True`, one uses a :class:`asyncio.Condition`.
    :meth:`Condition.wait_for(predicate) <asyncio.Condition.wait_for>` will block the coroutine until the ``predicate``
    returns `True` -- ``predicate`` will be reevaluated every time the condition
    :meth:`notifies <asyncio.Condition.notify>` waiting coroutines.

    An :class:`awaitable_predicate` object does exactly that, but it can also be evaluated to a boolean to make
    code more concise

    Example:

        >>> from ubii.framework import util
        >>> value = 0
        >>> is_zero = util.awaitable_predicate(lambda: value == 0)
        >>> bool(is_zero)
        True
        >>> value = 1
        >>> bool(is_zero)
        False

        Or we can `wait` until the predicate is actually `True`

        >>> [...]  # continued from above
        >>> async def set_value(number):
        ...     global value
        ...     async with is_zero.condition:
        ...             value = number
        ...             is_zero.condition.notify()
        ...
        >>> async def wait_for_zero():
        ...     await is_zero
        ...     print(f"Finally! value: {value}")
        ...
        >>> import asyncio
        >>> async def main():
        ...     asyncio.create_task(wait_for_zero())
        ...     for n in reversed(range(3)):
        ...             await set_value(n)
        ...
        >>> asyncio.run(main())
        Finally! value: 0

    """

    def __init__(self, predicate: typing.Callable[[], bool], condition: asyncio.Condition | None = None):
        self.condition = condition or asyncio.Condition()
        self.predicate = predicate
        self.waiting = None

    async def _waiter(self):
        async with self.condition:
            await self.condition.wait_for(self.predicate)

    def __await__(self):
        if self.waiting is None:
            self.waiting = self._waiter()

        return self.waiting.__await__()

    def __bool__(self):
        return self.predicate()


class make_dict(typing.Generic[S, T]):
    """
    This callable creates dictionaries using a key-function and a value-function
    to extract information from the values produced by some iterable.

    Example:

        >>> from ubii.framework import util
        >>> from collections import namedtuple
        >>> from random import sample
        >>> Foo = namedtuple('Foo', ['id', 'value'])
        >>> def random_foo(k):
        ...     return [Foo(*v) for v in zip(range(k), sample(range(k), k))]
        ...
        >>> id_to_value = util.make_dict(key=lambda f: f.id, value=lambda f: f.value)
        >>> some_foos = random_foo(5)
        >>> some_foos
        [Foo(id=0, value=2), Foo(id=1, value=0), Foo(id=2, value=1), Foo(id=3, value=3), Foo(id=4, value=4)]
        >>> id_to_value(some_foos)
        {0: 2, 1: 0, 2: 1, 3: 3, 4: 4}
        >>> more_foos = random_foo(3)
        >>> more_foos
        [Foo(id=0, value=0), Foo(id=1, value=2), Foo(id=2, value=1)]
        >>> id_to_value(more_foos)
        {0: 0, 1: 2, 2: 1}


    """

    def __init__(self: make_dict[S, T],
                 key: typing.Callable[[typing.Any], S],
                 value: typing.Callable[[typing.Any], T],
                 filter_none=False):
        self._key = key
        self._value = value
        self._filter = filter_none

    def __call__(self, iterable: typing.Iterable) -> typing.Dict[S, T]:
        return {
            self._key(item): self._value(item)
            for item in iterable
            if not self._filter or self._value(item)
        }


class async_compose:
    """
    Like :class:`compose` for coroutines.
    Coroutines are awaited in the order in which they are passed to :class:`async_compose` i.e.
    ``composed = async_compose(f, g)`` is equivalent to ``async def composed(*args): return await g(await f(*args))``

    Example:

        >>> from ubii.framework import util
        >>> import asyncio, random
        >>> async def simulate_IO(value):
        ...     await asyncio.sleep(random.random())
        ...     return value
        ...
        >>> async def make_values(k):
        ...     for n in range(k):
        ...             yield await simulate_IO(n)
        ...
        >>> async def process(values):
        ...     return [value async for value in values if value % 2 == 0]
        ...
        >>> async def result(values):
        ...     for value in values:
        ...             print(await simulate_IO(value))
        ...
        >>> processed = util.compose(make_values, process)  # make_values does not return a coroutine -> compose
        >>> composed = util.async_compose(processed, result) # processed returns a coroutine -> async_compose
        >>> asyncio.run(composed(5))
        0
        2
        4


    See Also:
        :class:`function_chain` -- if you want to pass the same argument to every function instead of composing

        :class:`compose` -- if you want to compose normal callables instead of coroutines

    """

    def __init__(self, *fns):
        def __compose(f: typing.Callable[[typing.Any], typing.Coroutine],
                      g: typing.Callable[[typing.Any], typing.Coroutine]):
            async def composed(*args):
                return await(g(await f(*args)))

            return composed

        self._reduced = functools.reduce(__compose, fns)

    def __call__(self, *args):
        return self._reduced(*args)


class enrich(typing.Callable[..., 'enrich.result']):
    """
    Enriches the results of a callable with some meta information.

    Useful if you deal with callables which you can't or don't want to change, to avoid
    leaking dependencies -- e.g. the part of the codebase that "knows" the meta information is not
    the same that defines the callables, and it's undesirable to change that.

    Works with normal callables and also coroutine functions -- if ``async`` coroutine functions / methods
    are used, the resulting callable will `await` the result first, before it adds the meta information and
    returns it to the caller.

    Example:

        Consider a mapping of callables:

        >>> import functools
        >>> make_print = functools.partial(functools.partial, print)
        >>> call_mapping = {value: make_print(value) for value in ['foo', 'bar', 'foobar']}
        >>> call_mapping['foo']()
        foo

        Pretend that ``make_print`` is defined somewhere else in your code, maybe even in someone else's code.
        You know that it's a factory for callables that print one value, and the code where you define the
        call mapping also knows which value each callable in the mapping will print.

        You don't want to pass this meta information around in your code (let's say as
        additional arguments for your calls), just because the implementation of ``make_print`` does not behave
        the way you want -- i.e. it does not also return the value that gets printed.

        So instead, you could define your mapping like this:

        >>> from ubii.framework import util
        >>> call_mapping = {v: util.enrich(v, make_print(v)) for v in ['foo', 'bar', 'foobar']}
        >>> result = call_mapping['foo']()
        foo
        >>> result
        result(value=None, meta='foo')

        Now your code can simply deal with the "fixed" callables.

    """
    result = namedtuple('result', ['value', 'meta'])
    """
    Named tuple for access of original return value and meta information
    """

    def __init__(self, meta, func: typing.Callable):
        """
        Add meta information to results of callable

        Args:
            meta: Something that each call to this callable should return in the :attr:`~.result.meta` field
            func: Simple callable or coroutine function. Result will be available as :attr:`~.result.value` field
        """
        self._reduced: typing.Union[async_compose, compose]
        if asyncio.iscoroutinefunction(func):
            async def attach(value):
                return self.result(value=value, meta=meta)

            self._reduced = async_compose(func, attach)
        else:
            def attach(value):
                return self.result(value=value, meta=meta)

            self._reduced = compose(func, attach)

    def __call__(self, *args):
        return self._reduced(*args)


class dunder:
    """
    the following decorators can be used to create dunder methods on classes.

    Note:

        Why not use `attrs <https://www.attrs.org/>`_?
            *   because the ubii framework should be usable by people that don't have a lot of experience in python, so
                it uses the standard library whenever possible -> dataclasses instead of `attrs`
            *   dataclasses are not useful for complex classes, nonetheless even a very complex class might have
                very easy `dunder` methods
    """

    @classmethod
    def all(cls, *attrs):
        """
        A decorator that applies all decorators from :class:`dunder`

        Args:
            *attrs: the names of attributes that should be part of the dunder methods

        Returns:
            a class decorator
        """
        return compose(cls.repr(*attrs), cls.hash(*attrs))

    @classmethod
    def repr(cls, *attrs):
        """
        Creates a __repr__ method on the class, that is basically ::

            def __repr__(self):
                info = {attr: getattr(self, attr, None) for attr in attrs}
                return f"{self.__class__.__name__}({', '.join('{}={}'.format(k, v) for k,v in info.items())})"

        Args:
            *attrs: the names of attributes that should be part of the __repr__

        Returns:
            a class decorator
        """

        def __repr__(instance):
            info = {attr: getattr(instance, attr, None) for attr in attrs}
            return (f"<{instance.__class__.__module__}.{instance.__class__.__name__} "
                    f"with {', '.join('{}={!r}'.format(k, v) for k, v in info.items())}>")

        def decorator(kls):
            kls.__repr__ = functools.wraps(kls.__repr__)(__repr__)
            return kls

        return decorator

    @classmethod
    def hash(cls, *attrs):
        """
        Creates a __hash__ method on the class, that is basically ::

            def __hash__(self):
                return hash((self.__class__, ) + tuple(map(lambda attr: getattr(self, attr, None), sorted(attrs))))

        Args:
            *attrs: the names of attributes that should be part of the __hash__

        Returns:
            a class decorator
        """

        def __hash__(instance):
            return hash((instance.__class__,) + tuple(map(lambda attr: getattr(instance, attr, None), sorted(attrs))))

        def decorator(kls):
            kls.__hash__ = functools.wraps(kls.__hash__)(__hash__)
            return kls

        return decorator


__all__ = (
    'similar',
    'hook',
    'registry',
    'exc_handler_decorator',
    'log_call',
    'ProtoRegistry',
    'function_chain',
    'compose',
    'awaitable_predicate',
    'make_dict',
    'async_compose',
    'enrich',
    'calc_delta',
    'dunder',
    'document_decorator'
)
