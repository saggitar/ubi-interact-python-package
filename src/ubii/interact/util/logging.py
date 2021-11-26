import argparse
import logging.config
import sys
import yaml
import ubii.interact
from importlib.resources import read_text

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
    ubii.interact.debug(args.debug)
