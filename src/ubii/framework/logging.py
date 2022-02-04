from __future__ import annotations

import argparse
import logging.config
import sys
import typing as t
from collections import namedtuple
from importlib.resources import read_text

import yaml

from . import util

__config__ = yaml.safe_load(read_text(util, 'logging_config.yaml'))


class _logging_setup:
    log_config = namedtuple('log_config', ['config', 'level', 'warning_filter'])

    def __init__(self, base_config=__config__, log_level=logging.INFO, warning_filter: str = 'always'):
        self.base_config = self.log_config(config=base_config, warning_filter=warning_filter, level=log_level)
        self._configs: t.List[_logging_setup.log_config] = [self.base_config]
        self._applied = False

    @property
    def effective_config(self):
        return self.log_config(**self._configs[-1]._asdict())

    def change(self, config: t.Dict | None = None, verbosity: int | None = None):
        if config is None and verbosity is None:
            raise ValueError(f"All arguments are None, can' apply change to logging.")

        verbosity = self.effective_config.level if verbosity is None else verbosity

        config = config or {}

        if config.get('incremental', False):
            config = util.merge_dicts(self.effective_config.config, config)
            config['incremental'] = False  # we do the merging, logging framework should just eat the config

        config = config or self.effective_config.config
        config['version'] = 1  # other versions are not supported by the logging framework

        new_config = self.log_config(level=verbosity,
                                     config=config,
                                     warning_filter=self.effective_config.warning_filter)

        if self._applied:
            self._configs.append(new_config)
        else:
            self._configs = [new_config]

        return self

    def _apply(self):
        self._applied = True

        logging.config.dictConfig(self.effective_config.config)
        logging.captureWarnings(True)
        logging.getLogger().setLevel(level=self.effective_config.level)

        if not sys.warnoptions:
            import os, warnings
            warnings.simplefilter(self.effective_config.warning_filter)  # Change the filter in this process
            os.environ["PYTHONWARNINGS"] = self.effective_config.warning_filter  # Also affect subprocesses

    def reset(self):
        self._configs[:] = [self._configs[0]]
        self._apply()

    def __enter__(self):
        self._apply()
        return self

    def __exit__(self, *exc_info):
        if any(exc_info):
            self._configs.clear()
        else:
            _ = self._configs.pop()

        if self._configs:
            self._apply()


logging_setup = _logging_setup(base_config=__config__, log_level=logging.INFO, warning_filter='default')


def parse_args(parser=None):
    parser = parser or argparse.ArgumentParser()

    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--log-config', action='store', default=__config__)
    args = parser.parse_args()

    verbosity = logging.INFO - 10 * args.verbose
    debug(args.debug)

    if debug():
        verbosity = min(logging.DEBUG, verbosity)

    logging_setup.change(config=args.log_config, verbosity=verbosity)

    return args




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
