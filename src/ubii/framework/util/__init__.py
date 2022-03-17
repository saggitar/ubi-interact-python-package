from __future__ import annotations

from codestare.async_utils import (
    accessor,
    condition_property,
    make_async,
    CoroutineWrapper,
    TaskNursery,
    async_exit_on_exc,
    RegistryMeta,
    Registry,
)
from .collections import merge_dicts
from .enum import EnumMatcher
from .functools import (
    similar,
    hook,
    registry,
    exc_handler_decorator,
    calc_delta,
    log_call,
    ProtoRegistry,
    function_chain,
    compose,
    awaitable_predicate,
    make_dict,
    async_compose,
    attach_info,
    AbstractAnnotations,
    document_decorator,
)

__DEBUG__ = False


def debug(enabled: bool | None = None):
    """
    Call without arguments to get current debug state, pass truthy value to set debug mode.

    :param enabled: If passed, turns debug mode on or off
    :return:
    """
    global __DEBUG__
    if enabled is not None:
        __DEBUG__ = bool(enabled)

    return __DEBUG__


__all__ = (
    "accessor",
    "condition_property",
    "make_async",
    "CoroutineWrapper",
    "TaskNursery",
    "async_exit_on_exc",
    "RegistryMeta",
    "Registry",
    "similar",
    "hook",
    "registry",
    "exc_handler_decorator",
    "log_call",
    "ProtoRegistry",
    "function_chain",
    "compose",
    "awaitable_predicate",
    "make_dict",
    "merge_dicts",
    "async_compose",
    "attach_info",
    "calc_delta",
    "AbstractAnnotations",
    "debug",
    "document_decorator"
)
