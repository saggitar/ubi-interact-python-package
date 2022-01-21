from __future__ import annotations

import typing as t

from ubii.interact import client, constants, protocol, default_protocol


class connect(t.Awaitable[client.UbiiClient[protocol.StandardProtocol]],
              t.AsyncContextManager[client.UbiiClient[protocol.StandardProtocol]]):
    class ClientFactory(t.Protocol):
        def __call__(self,
                     instance: connect,
                     *,
                     client_type: t.Type[client.UbiiClient],
                     protocol_type: t.Type[protocol.UbiiProtocol]) -> client.UbiiClient[protocol.UbiiProtocol]: ...

    def __init__(self,
                 url=None,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 client_type: t.Type[client.UbiiClient] = client.UbiiClient,
                 protocol_type: t.Type[protocol.UbiiProtocol] = default_protocol.DefaultProtocol):
        if url is not None:
            config.DEFAULT_SERVICE_URL = url
        self.config = config
        factory = self.client_factories.get((client_type, protocol_type))
        if not factory:
            raise ValueError(f"No factory found for {(client_type, protocol_type)}")

        self.client = factory(
            self,
            client_type=client_type,
            protocol_type=protocol_type
        )

    def default_create(self, *, client_type, protocol_type):
        if client_type == client.UbiiClient and protocol_type == default_protocol.DefaultProtocol:
            _protocol = protocol_type(config=self.config)
            _client = client_type(protocol=_protocol)
            _protocol.client = _client
            return _client
        else:
            raise NotImplementedError(f"{self.default_create} can't create a client for client type {client_type} "
                                      f"and protocol type {protocol_type}.")

    def __await__(self) -> t.Generator[t.Any, None, client.UbiiClient]:
        return self.client.__await__()

    def __aenter__(self):
        return self.client.__aenter__()

    def __aexit__(self, *exc_infos):
        return self.client.__aexit__(*exc_infos)

    def __enter__(self):
        return self.client

    def __exit__(self, *exc_info):
        self.client.protocol.task_nursery.create_task(self.client.protocol.stop())

    client_factories: t.Dict[t.Tuple[t.Type[client.UbiiClient], t.Type[protocol.UbiiProtocol]], ClientFactory] = {
        (client.UbiiClient, default_protocol.DefaultProtocol): default_create
    }
