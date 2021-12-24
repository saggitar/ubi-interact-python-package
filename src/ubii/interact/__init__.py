from ._default import StandardProtocol

__DEBUG__ = False


def debug(enabled: bool = None):
    """
    Call without arguments to get current debug state, pass truthy value to set debug mode.

    :param enabled: If passed, turns debug mode on or off
    :return:
    """
    global __DEBUG__
    if enabled is not None:
        __DEBUG__ = bool(enabled)

    return __DEBUG__


__all__ = [
    "debug",
]
