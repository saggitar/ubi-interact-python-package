from __future__ import annotations

import argparse
import collections
import functools
import importlib.resources
import logging.config
import warnings
from typing import (
    Dict,
    List,
)

import sys
import yaml

from . import util

__config__ = yaml.safe_load(importlib.resources.read_text(util, 'logging_config.yaml'))


class _logging_setup:
    log_config = collections.namedtuple('log_config', ['config', 'level', 'warning_filter'])

    @staticmethod
    def add_log_level(name: str, value: int):
        """
        Adds a custom log level to the logging module

        Args:
            value: logging level
            name: name of the new level

        """
        callback_name = name.lower()
        logger_kls = logging.getLoggerClass()
        existing = {
            obj: getattr(obj, attr, None)
            for obj in (logging, logger_kls)
            for attr in (name, callback_name)
        }
        if any(existing.values()):
            warnings.warn(
                f"Can't configure new log level, "
                f"attributes found: {','.join(f'{k} -> {v}' for k,v in existing.items() if v)}"
            )
            return

        assert hasattr(logger_kls, 'log')
        logging.addLevelName(value, name)
        setattr(logging, name, value)
        setattr(logger_kls, callback_name, functools.partialmethod(logger_kls.log, value))
        setattr(logging, callback_name, functools.partial(logging.log, value))

    def __init__(self, base_config=__config__, log_level=logging.INFO, warning_filter: str = 'always'):
        if not hasattr(logging, 'VERBOSE'):
            self.add_log_level('VERBOSE', logging.DEBUG - 5)

        self.base_config = self.log_config(config=base_config, warning_filter=warning_filter, level=log_level)
        self._configs: List[_logging_setup.log_config] = [self.base_config]
        self._applied = False

    @property
    def effective_config(self):
        return self.log_config(**self._configs[-1]._asdict())

    def change(self, config: Dict | None = None, verbosity: int | None = None):
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
        config = self._configs.pop()
        if any(exc_info):
            self._configs.clear()

        if self._configs:
            self._apply()
        else:
            self._configs[:] = [config]


logging_setup = _logging_setup(base_config=__config__, log_level=logging.INFO, warning_filter='default')
"""

"""


def parse_args(parser: argparse.ArgumentParser | None = None) -> argparse.Namespace:
    """
    Convenience function to parse command line arguments.
    Adds the following command line arguments to the parser:

        *   ``--verbose``, ``-v`` -- can be specified multiple times, increases the verbosity of the framework logging
        *   ``--debug`` -- sets the framework to :func:`~ubii.framework.debug` mode
        *   ``--log-config`` -- takes a `yaml` file containing a dictionary for :func:`logging.config.dictConfig`
            configuration, an example config is available at
            https://github.com/saggitar/ubii-node-python/blob/develop/src/ubii/framework/util/logging_config.yaml

    Example:

        ::

            from ubii.framework.logging import parse_args
            import argparse

            parser = argparse.ArgumentParser()
            parser.add_argument('--processing-modules', action='append', default=[])
            parser.add_argument('--no-discover', action='store_true', default=False)
            args = parse_args(parser=parser)

    Args:
        parser: if no parser is passed, a new one is created  -- `optional`

    Returns:
        parsed arguments
    """
    parser = parser or argparse.ArgumentParser()

    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--log-config', action='store', default=None)
    args = parser.parse_args()

    verbosity = logging.INFO - 10 * args.verbose
    util.debug(args.debug)

    if util.debug():
        verbosity = min(logging.DEBUG, verbosity)

    if args.log_config:
        with open(args.log_config) as conf:
            args.log_config = yaml.safe_load(conf)
    else:
        args.log_config = __config__

    logging_setup.change(config=args.log_config, verbosity=verbosity)

    return args
