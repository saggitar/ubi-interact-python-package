import asyncio
import logging
import typing
import warnings
from pathlib import Path

from ubii.framework.client import UbiiClient
from ubii.framework.protocol import AbstractProtocol

try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata

log = logging.getLogger(__name__)

SETUPTOOLS_PM_ENTRYPOINT_KEY = 'ubii.processing_modules'
"""
Processing modules need to register their entry points with this key
"""


def import_name(name: str):
    """
    Utility method, imports the module where the name is located

    Example::

        client_type = import_name('ubii.framework.client.UbiiClient')

    Args:
        name: full name, e.g. 'the.module.attribute'

    Returns:
        the attribute with specified name, from the imported module
    """
    module, attribute = name.rsplit('.', maxsplit=1)
    assert module
    assert attribute
    from importlib import import_module
    module = import_module(module)
    return getattr(module, attribute)


def log_to_folder(log_config, folder='logs/'):
    """
    Changes the log config so that all logs are written to the specified folder
    Args:
        folder: path to folder that should be prefixed
        log_config: configuration dictionary in :func:`logging.config.dictConfig` format

    Returns:
        changed dictionary
    """
    for name, config in log_config['handlers'].items():
        log_file = config.get('filename', None)
        if log_file is None:
            continue

        if not log_file.startswith(folder):
            log_file = Path(f"{folder}{log_file}")
            config['filename'] = str(log_file)

        log_file.parent.mkdir(parents=True, exist_ok=True)

    return log_config


def load_pm_entry_points() -> typing.List[typing.Any]:
    """
    Loads setuptools entrypoints for key :attr:`SETUPTOOLS_PM_ENTRYPOINT_KEY`

    Returns:
        list of :class:`~ubii.framework.processing.ProcessingRoutine` types
    """
    with warnings.catch_warnings():
        # this deprecation is discussed a lot
        warnings.simplefilter("ignore")
        return [entry.load() for entry in metadata.entry_points().get(SETUPTOOLS_PM_ENTRYPOINT_KEY, ())]


def main():
    """
    Entry point for cli script see :ref:`CLI` in the documentation
    """
    import argparse
    from ubii.node import connect_client
    from ubii.framework.logging import parse_args, logging_setup
    from ubii.framework.client import InitProcessingModules
    from codestare.async_utils.nursery import TaskNursery

    parser = argparse.ArgumentParser()
    parser.add_argument('--processing-modules', action='append', default=[])
    parser.add_argument('--no-discover', action='store_true', default=False)

    args = parse_args(parser=parser)
    log_config = logging_setup.change(
        config=log_to_folder(logging_setup.effective_config.config)
    )

    pms = set()
    if not args.no_discover:
        pms.update(load_pm_entry_points())
    if args.processing_modules:
        pms.update(import_name(name) for name in args.processing_modules)

    if pms:
        print(f"Imported {', '.join(map(repr, pms))}")
    else:
        warnings.warn(f"No processing modules imported")

    async def run():
        with connect_client() as client:
            client.is_dedicated_processing_node = True
            client[InitProcessingModules] = InitProcessingModules(module_types=pms, initialized=[])
            assert client.implements(InitProcessingModules)
            await client

            while client.state != client.State.UNAVAILABLE:
                await asyncio.sleep(1)

        loop.stop()

    loop = asyncio.get_event_loop_policy().get_event_loop()
    nursery = TaskNursery(name="__main__", loop=loop)

    with log_config:
        nursery.create_task(run())
        loop.run_forever()

    assert not loop.is_running()
    loop.close()


def info_log_client():
    """
    Example for tutorial to create a simple client that prints messages received in the info topics,
    and continuously publishes a value to a custom info topic. See :ref:`Client Example`
    """
    import asyncio
    import argparse
    from ubii.framework.logging import parse_args
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, action='store', default=None, help='URL of master node service endpoint')
    args = parse_args(parser=parser)

    from ubii.node import connect_client, Subscriptions, Publish, DefaultProtocol

    loop = asyncio.get_event_loop_policy().get_event_loop()

    async def run():
        client: UbiiClient[DefaultProtocol]
        async with connect_client(url=args.url) as client:
            # we don't hard code the topic, we use the DEFAULT TOPIC from the master node
            constants = client.protocol.context.constants
            assert constants
            info_topic_pattern = constants.DEFAULT_TOPICS.INFO_TOPICS.REGEX_ALL_INFOS
            info, = await client[Subscriptions].subscribe_regex(info_topic_pattern)

            print(f"Subscribed to topic {info_topic_pattern!r}")
            info.register_callback(print)

            value = None

            while client.state != client.State.UNAVAILABLE:
                value = 'foo' if value == 'bar' else 'bar'
                await asyncio.sleep(1)
                await client[Publish].publish({'topic': '/info/custom_topic', 'string': value})

        loop.stop()

    from codestare.async_utils.nursery import TaskNursery
    nursery = TaskNursery(name="__main__", loop=loop)
    nursery.create_task(run())
    loop.run_forever()

    assert not loop.is_running()
    loop.close()


if __name__ == '__main__':
    main()
