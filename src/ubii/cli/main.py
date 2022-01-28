import argparse

from warnings import warn

import asyncio
from pathlib import Path


def import_pm(import_name: str):
    module, attribute = import_name.rsplit('.', maxsplit=1)
    assert module
    assert attribute
    from importlib import import_module
    module = import_module(module)
    return getattr(module, attribute)


def log_to_folder(log_config):
    for name, config in log_config['handlers'].items():
        log_file = config.get('filename', None)
        if log_file is None:
            continue

        if not log_file.startswith('logs/'):
            log_file = Path(f"logs/{log_file}")
            config['filename'] = str(log_file)

        log_file.parent.mkdir(parents=True, exist_ok=True)

    return log_config


def main():
    from ubii.interact import connect_client
    from ubii.interact.logging import parse_args, logging_setup
    from ubii.interact.client import InitProcessingModules
    parser = argparse.ArgumentParser()
    parser.add_argument('--processing-modules', action='append', default=[])

    args = parse_args(parser=parser)

    log_config = logging_setup.change(config=log_to_folder(args.log_config))

    pms = [import_pm(name) for name in args.processing_modules]
    if pms:
        print(f"Imported {', '.join(map(repr, pms))}")
    else:
        warn(f"No processing modules imported")

    async def run():
        with connect_client() as client:
            client.is_dedicated_processing_node = True
            assert client.implements(InitProcessingModules)
            client[InitProcessingModules].late_init_processing_modules = pms
            await client

            while True:
                await asyncio.sleep(5)
                print(".")

    loop = asyncio.get_event_loop_policy().get_event_loop()
    with log_config:
        task = loop.create_task(run())
        loop.run_forever()

    assert task.done()
    assert not loop.is_running()
    loop.close()


if __name__ == '__main__':
    main()
