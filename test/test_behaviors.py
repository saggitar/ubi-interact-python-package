from __future__ import annotations

import asyncio
import dataclasses

from ubii.framework.client import UbiiClient
from ubii.node.protocol import DefaultProtocol


class TestClientBehavior:

    async def test_behaviors(self, client, configure_logging):
        await client
        behaviors = client.behaviors
        assert 'optional_behaviors' in behaviors
        assert 'required_behaviors' in behaviors
        assert all(client.implements(b) for b in behaviors['required_behaviors'])

        @dataclasses.dataclass
        class FooBehavior:
            foo: int | None = None

        behaviors['optional_behaviors'] += (FooBehavior,)

        new_protocol: DefaultProtocol = type(client.protocol)()
        new_client = UbiiClient(
            protocol=new_protocol,
            **behaviors
        )
        new_protocol.client = new_client
        new_client = await asyncio.wait_for(new_client, timeout=2)

        assert new_client.id
        assert not new_client.implements(FooBehavior)
        new_client[FooBehavior].foo = 42
        assert new_client.implements(FooBehavior)