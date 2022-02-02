from __future__ import annotations

import logging
import typing as t

import ubii.framework.client
from . import node_protocol as default_protocol_
from ubii.framework import (
    client as client_,
    constants as constants_,
    protocol as protocol_,
)
from ubii.framework.util.typing import Protocol

log = logging.getLogger(__name__)


class connect(t.Awaitable[client_.UbiiClient[ubii.framework.client.AbstractClientProtocol]],
              t.AsyncContextManager[client_.UbiiClient[ubii.framework.client.AbstractClientProtocol]]):
    class ClientFactory(Protocol):
        def __call__(self,
                     instance: connect,
                     *,
                     client_type: t.Type[client_.UbiiClient],
                     protocol_type: t.Type[protocol_.AbstractProtocol]) -> client_.UbiiClient[
            protocol_.AbstractProtocol]: ...

    def __init__(self,
                 url=None,
                 config: constants_.UbiiConfig = constants_.GLOBAL_CONFIG,
                 client_type: t.Type[client_.UbiiClient] = client_.UbiiClient,
                 protocol_type: t.Type[protocol_.AbstractProtocol] = default_protocol_.DefaultProtocol):
        if url is not None:
            config.DEFAULT_SERVICE_URL = url
        self.config = config
        factory = self.client_factories.get((client_type, protocol_type))
        if not factory:
            raise ValueError(f"No client factory found for {(client_type, protocol_type)}")
        else:
            log.debug(f"Using {factory} to create {client_type} with {protocol_type}")

        self.client = factory(
            self,
            client_type=client_type,
            protocol_type=protocol_type
        )

    def default_create(self, *, client_type, protocol_type):
        protocol = protocol_type(config=self.config)
        client = client_type(protocol=protocol)
        protocol.client = client
        return client

    def __await__(self) -> t.Generator[t.Any, None, client_.UbiiClient]:
        return self.client.__await__()

    def __aenter__(self):
        return self.client.__aenter__()

    def __aexit__(self, *exc_infos):
        return self.client.__aexit__(*exc_infos)

    def __enter__(self):
        return self.client

    def __exit__(self, *exc_info):
        self.client.protocol.task_nursery.create_task(self.client.protocol.stop())

    client_factories: t.Dict[t.Tuple[t.Type[client_.UbiiClient], t.Type[protocol_.AbstractProtocol]], ClientFactory] = {
        (client_.UbiiClient, default_protocol_.DefaultProtocol): default_create
    }
