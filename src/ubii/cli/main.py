import asyncio

from contextlib import suppress


def import_pm(import_name: str):
    module, attribute = import_name.rsplit('.', maxsplit=1)
    assert module
    assert attribute
    from importlib import import_module
    module = import_module(module)
    return getattr(module, attribute)


def main():
    from ubii.interact import connect_client, debug
    from ubii.interact.logging import parse_args
    from ubii.interact.client import InitProcessingModules

    args = parse_args()
    pms = [import_pm(name) for name in args.processing_modules]

    async def run():
        with connect_client() as client:
            client.is_dedicated_processing_node = True
            assert client.implements(InitProcessingModules)
            client[InitProcessingModules].late_init_processing_modules = []
            await client

            while True:
                await asyncio.sleep(5)
                print("<Ping>")

    with suppress(asyncio.CancelledError):
        asyncio.run(run(), debug=debug())


if __name__ == '__main__':
    main()
