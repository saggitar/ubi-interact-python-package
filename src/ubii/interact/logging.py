from __future__ import annotations

import argparse
import logging.config
import re
import sys
import yaml
from functools import wraps
from importlib.resources import read_text

import ubii.interact
import ubii.proto as ub

__config__ = yaml.safe_load(read_text(ubii.interact, 'logging_config.yaml'))


def set_logging(config=__config__, verbosity=logging.INFO):
    logging.config.dictConfig(config)
    logging.getLogger().setLevel(level=verbosity)
    logging.captureWarnings(True)

    if not sys.warnoptions:
        import os, warnings
        warnings.simplefilter("default")  # Change the filter in this process
        os.environ["PYTHONWARNINGS"] = "default"  # Also affect subprocesses


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()

    set_logging(verbosity=logging.ERROR - 10 * args.verbose)
    debug(args.debug)


def shorten_json(message: str, max_len=50):
    """
    Format json strings (like representations of proto messages) in a nice way.

    :param message:
    :param max_len:
    :return:
    """
    cleaned = message.strip()
    formatted = re.sub(r'{\s+', '{', cleaned)
    formatted = re.sub(r'\n', ' | ', formatted)
    formatted = re.sub(r'\s', '', formatted)
    placeholder = ' ... '

    def _format_final(_result):
        _result = re.sub(r'\|', ', ', _result)
        total = max_len - len(placeholder)
        if placeholder in _result:
            return _result
        else:
            return _result[:total // 2] + placeholder + _result[total // 2:]

    if '|' not in formatted:
        return _format_final(formatted)

    result = ''
    left = 0

    while len(result) < max_len:
        try:
            left = formatted.index('|', left) + 1
        except ValueError:
            break

        result = formatted[:left + 1] + placeholder + formatted[-1]

    return _format_final(result)


class ProtoFormatMixin:
    _MAX_REPR_LEN = 100

    def __str__(self: ub.ProtoMessage):
        contents = shorten_json(super().__str__(), max_len=self._MAX_REPR_LEN)
        return f"<{type(self).__name__}{' ' + contents if contents else ''}>"


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