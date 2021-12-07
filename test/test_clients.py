import asyncio

import pytest

from ubii.interact.client.node import Client
from ubii.interact.hub import Ubii

pytestmark = pytest.mark.asyncio

class TestClients:
    async def test_registration(self):
        from ubii.interact.client.node import Client
        name = 'Ubii Python Test Node'
        async with Client(name=name).initialize() as node:
            assert node.id and node.name == name
            node_id = node.id
            await asyncio.sleep(10)  # hold connection for 10 seconds without failure

        # we explicitly don't request the ubii instance, so that we can test if deregistering
        # the node on context exit works if there is no additional reference to the Ubii instance
        # to keep the http session from closing prematurely.
        # the session should be closed now
        from ubii.interact.hub import Ubii
        hub = Ubii.instance
        assert hub.client_session.closed

        # we want to ask the server if the node is still registered, so we have to reset the client session.
        del hub.client_session  # this clears the cached property
        async with hub.initialize():
            clients = await hub.services.client_get_list()
            assert not any(n.id == node_id for n in clients.elements), \
                f"Client with id {node_id} was not deregistered successfully"