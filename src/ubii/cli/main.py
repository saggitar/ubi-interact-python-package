import argparse
import ast
import asyncio
import functools
import logging
import typing
import warnings
from pathlib import Path

from ubii.framework.client import UbiiClient

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
            log_file = f"{folder}{log_file}"

        log_file = Path(log_file)
        config['filename'] = str(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

    return log_config


def load_pm_entry_points() -> typing.Dict[str, typing.Any]:
    """
    Loads setuptools entrypoints for key :attr:`SETUPTOOLS_PM_ENTRYPOINT_KEY`

    Returns:
        list of :class:`~ubii.framework.processing.ProcessingRoutine` types
    """
    with warnings.catch_warnings():
        # this deprecation is discussed a lot
        warnings.simplefilter("ignore")
        entries = [entry for entry in metadata.entry_points().get(SETUPTOOLS_PM_ENTRYPOINT_KEY, ())]
        return {entry.name: entry.load() for entry in entries}


def parse_args():
    """
    Enhanced argument parsing which allows to also load installed processing
    modules and pass arguments to them when they are loaded by the client
    """
    import argparse
    import re
    from ubii.framework.logging import parse_args

    def pm_type(arg_value, pat=r'(?:(.*):)?(.*)'):
        matched = re.match(pat, arg_value)
        if not matched:
            raise argparse.ArgumentTypeError(f"{arg_value} has wrong format, allowed: {pat}")

        return matched.groups()

    kv = r'(?:.*)=(?:.*)'  # a key value pair regex, only capturing the pair

    def mod_arg_type(arg_value, pat=r'(?:(.*):)?({kv}(?:,{kv})*)'.format(kv=kv)):
        matched = re.match(pat, arg_value)
        if not matched:
            raise argparse.ArgumentTypeError(f"{arg_value} has wrong format, allowed: {pat}")

        name, args = matched.groups()
        argdict = {}
        for a in args.split(','):
            key, value = a.rsplit('=', maxsplit=1)
            argdict[key] = ast.literal_eval(value)

        return name, argdict

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--processing-modules', action='append', default=[],
        type=pm_type,
        help='Import processing modules to load. '
             'You can also use the format {name}:{type} to load them with specific names.'
    )
    parser.add_argument(
        '--no-discover', action='store_true', default=False,
        help="Don't use the automatic processing module discovery mechanism"
    )
    parser.add_argument(
        '--module-args', action='append', default=[],
        type=mod_arg_type,
        help="Format {name}:{key}={value}(,{key}={value})*, e.g. my-module:tags=['custom'],description='Wow!'"
    )

    return parse_args(parser=parser)


def create_pm_factories(args: argparse.Namespace) -> typing.Dict[str, typing.Any]:
    pms = {}
    if not args.no_discover:
        pms.update(**load_pm_entry_points())
    if args.processing_modules:
        for name, value in args.processing_modules:
            pms[name] = import_name(value)

    for name, argdict in args.module_args:
        if name not in pms:
            warnings.warn(f"Arguments for module {name} specified but no module with that name was loaded")
            continue

        # update arguments
        pms[name] = functools.partial(pms[name], **argdict)

    if not pms:
        warnings.warn(f"No processing modules imported")
    else:
        print(f"Loaded processing module factories {', '.join(map(repr, pms.values()))}")

    return pms


def main():
    """
    Entry point for cli script see :ref:`CLI` in the documentation
    """
    from ubii.node import connect_client
    from ubii.framework.client import InitProcessingModules
    from codestare.async_utils.nursery import TaskNursery
    from ubii.framework import logging_setup

    pms = create_pm_factories(parse_args())

    log_config = logging_setup.change(
        config=log_to_folder(logging_setup.effective_config.config)
    )

    async def run():
        with connect_client() as client:
            try:
                client.is_dedicated_processing_node = True
                client[InitProcessingModules] = InitProcessingModules(module_factories=pms)
                assert client.implements(InitProcessingModules)
                await client
                while client.state != client.State.UNAVAILABLE:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

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

    from ubii.node import connect_client, Subscriptions, Publish, LegacyProtocol

    loop = asyncio.get_event_loop_policy().get_event_loop()

    async def run():
        client: UbiiClient[LegacyProtocol]
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
