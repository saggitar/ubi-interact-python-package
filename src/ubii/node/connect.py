from __future__ import annotations

import logging
import typing

from ubii.framework import (
    client,
    constants,
    protocol,
)
from ubii.framework.util.typing import Protocol
from . import protocol as default_protocol_

log = logging.getLogger(__name__)


class connect(typing.Awaitable[client.UbiiClient[client.AbstractClientProtocol]],
              typing.AsyncContextManager[client.UbiiClient[client.AbstractClientProtocol]]):
    """
    Use this callable to easily get a running Ubi Interact Node. If you don't want to change the
    node behaviour significantly, this is the go-to to connect with the `master node`.


    Example:

        Use the default :class:`connect` callable with defaults to connect to a `master node`.
        If :class:`connect` uses a ``url`` argument, it takes precedence over the
        :attr:`~ubii.framework.constants.UbiiConfig.DEFAULT_SERVICE_URL` of the ``config`` argument.
        If :class:`connect` does not use a specific constants, the shared
        :attr:`~ubii.framework.constants.GLOBAL_CONFIG` is used by default.

        There are multiple ways to create the client with :class:`connect`.

            with :ref:`await` ::

                from ubii.node.connect import connect
                import asyncio

                async def main():
                    global client
                    client = await connect()
                    assert client.id

                    # do something with the client
                    ...

                asyncio.run(main())


    """
    class ClientFactory(Protocol):
        def __call__(self,
                     instance: connect,
                     *,
                     client_type: typing.Type[client.UbiiClient],
                     protocol_type: typing.Type[protocol.AbstractProtocol]) -> client.UbiiClient[
            protocol.AbstractProtocol
        ]:
            """
            :class:`ClientFactory` objects need to have this call signature

            Args:
                instance: a :class:`connect` instance
                client_type: a :class:`~ubii.framework.client.UbiiClient` type
                protocol_type: a :class:`~ubii.framework.protocol.AbstractProtocol` type

            Returns:
                A client using the protocol
            """

    def __init__(self,
                 url=None,
                 config: constants.UbiiConfig = constants.GLOBAL_CONFIG,
                 client_type: typing.Type[client.UbiiClient] = client.UbiiClient,
                 protocol_type: typing.Type[protocol.AbstractProtocol] = default_protocol_.DefaultProtocol):
        """
        Args:
            url: URL of the `master node`, overwrites the
                :attr:`.ubii.framework.constants.UbiiConfig.DEFAULT_SERVICE_URL` of the used ``config``
            config: contains information like default topic for `server configuration` service call
            client_type: a :class:`~ubii.framework.client.UbiiClient` type
            protocol_type: a :class:`~ubii.framework.protocol.AbstractProtocol` type
        """
        if url is not None:
            config.DEFAULT_SERVICE_URL = url

        self.config: constants.UbiiConfig = config
        """
        reference to used config
        """

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
        """
        Will be created with a callable from :attr:`.client_factories` if it contains one for the 
        ``client_type`` and ``protocol_type``
        """

    def default_create(self, *, client_type, protocol_type):
        """
        Simply instantiates the ``protocol_type`` (passing :attr:`.config` as ``config``),
        creates a client with this protocol, and sets the protocols
        :attr:`~ubii.framework.client.AbstractClientProtocol.client` attribute

        Args:
            client_type:
            protocol_type:

        Returns:
            the client using the protocol
        """
        protocol = protocol_type(config=self.config)
        client = client_type(protocol=protocol)
        protocol.client = client
        return client

    def __await__(self) -> typing.Generator[typing.Any, None, client.UbiiClient]:
        return self.client.__await__()

    def __aenter__(self):
        return self.client.__aenter__()

    def __aexit__(self, *exc_infos):
        return self.client.__aexit__(*exc_infos)

    def __enter__(self):
        return self.client

    def __exit__(self, *exc_info):
        self.client.protocol.task_nursery.create_task(self.client.protocol.stop())

    client_factories: typing.Dict[
        typing.Tuple[typing.Type[client.UbiiClient], typing.Type[protocol.AbstractProtocol]], ClientFactory] = {
        (client.UbiiClient, default_protocol_.DefaultProtocol): default_create
    }
