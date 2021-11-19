from pathlib import Path

_log_config_ = 'logging_config.yaml'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()

    config = Path(_log_config_)
    with config.open('r') as config:
        logging.config.dictConfig(yaml.load(config, yaml.SafeLoader))
    logging.getLogger().setLevel(level=logging.ERROR - 10 * args.verbose)
    logging.captureWarnings(True)

    if not sys.warnoptions:
        import os, warnings
        warnings.simplefilter("default")  # Change the filter in this process
        os.environ["PYTHONWARNINGS"] = "default"  # Also affect subprocesses

    ubii_interact.enable_debug(args.debug)
