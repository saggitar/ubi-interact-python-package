from __future__ import annotations

import asyncio
import enum
import logging
import random
from dataclasses import dataclass
from typing import Callable

import pytest

from ubii.framework.client import UbiiClient
from ubii.framework.protocol import AbstractProtocol, RunProtocol
from ubii.node.protocol import DefaultProtocol, States as UbiiStates

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)


class MockProtocol(AbstractProtocol[UbiiStates]):
    starting_state = UbiiStates.STARTING
    end_state = UbiiStates.STOPPED

    async def _on_start(self, context):
        log.info(context)
        context.data = "test"
        await self.state.set(UbiiStates.CONNECTED)

    async def _on_connect(self, context):
        log.info(context)
        assert context.data == 'test'

        async def stop_at_some_point():
            await asyncio.sleep(random.randrange(5, 10))
            await self.state.set(UbiiStates.STOPPED)

        context.stop_task = asyncio.create_task(stop_at_some_point())

    async def _on_stopped(self, context):
        log.info(context)
        context.stop_task.cancel()
        await context.stop_task

    state_changes = {
        (None, UbiiStates.STARTING): _on_start,
        (UbiiStates.STARTING, UbiiStates.CONNECTED): _on_connect,
        (UbiiStates.CONNECTED, UbiiStates.STOPPED): _on_stopped,
    }


async def test_mock_protocol():
    protocol = MockProtocol()
    coro = RunProtocol(protocol)
    await asyncio.create_task(coro)


@pytest.fixture
async def protocol(request):
    protocol: AbstractProtocol = request.param()
    running = protocol.start()
    yield running
    await running.stop()


@pytest.mark.parametrize('protocol', [DefaultProtocol], indirect=True)
async def test_default_protocol(protocol: DefaultProtocol):
    await asyncio.wait_for(protocol.state.get(predicate=lambda state: state == UbiiStates.CONNECTED),
                           timeout=5)
    client = protocol.client
    assert client.id


@pytest.mark.parametrize('protocol', [DefaultProtocol], indirect=True)
async def test_default_protocol_stop(protocol: DefaultProtocol):
    await asyncio.wait_for(protocol.state.get(predicate=lambda state: state == UbiiStates.CONNECTED),
                           timeout=5)
    client = protocol.client
    assert client.id
    await protocol.stop()
    assert protocol.state.value == protocol.end_state


class TestProtocol(AbstractProtocol):
    class TestStates(enum.IntFlag):
        START = enum.auto()
        RUNNING = enum.auto()
        END = enum.auto()
        ANY = START | RUNNING | END

    async def on_start(self, context):
        print(f"starting with context:\n-> {context}")
        await asyncio.sleep(2)  # simulating some setup IO ...
        await self.state.set(self.TestStates.RUNNING)

    async def on_run(self, context):
        print(f"running with context:\n-> {context}")

    async def on_stop(self, context):
        print(f"stopping with context:\n-> {context}")

    starting_state = TestStates.START
    end_state = TestStates.END

    state_changes = {
        (None, TestStates.START): on_start,
        (TestStates.START, TestStates.RUNNING): on_run,
        (TestStates.ANY, TestStates.END): on_stop,
    }


@dataclass(init=True, repr=True, eq=True)
class FooBehaviour:
    foo: Callable[[str], None] | None = None


@dataclass(init=True, repr=True, eq=True)
class BarBehaviour:
    bar: str | None = None


class FakeClientProtocol(TestProtocol):
    TestStates = TestProtocol.TestStates

    def __init__(self):
        super().__init__()
        self.client: UbiiClient | None = None

    async def on_run(self, context):
        assert self.client is not None
        self.client[FooBehaviour].foo = print

    state_changes = {**TestProtocol.state_changes, (TestStates.START, TestStates.RUNNING): on_run}


async def test_fake_protocol():
    protocol = FakeClientProtocol()

    client = UbiiClient(
        name='Test Client',
        required_behaviours=(FooBehaviour,),
        optional_behaviours=(BarBehaviour,),
        protocol=protocol
    )
    protocol.client = client

    async def use_behaviours():
        await client.implements(BarBehaviour, FooBehaviour)
        client[FooBehaviour].foo(f"Using behaviours -> {client[BarBehaviour].bar}")

    async def implement_bar():
        client[BarBehaviour].bar = 'Bar'

    async with client as started_client:
        assert not started_client.implements(BarBehaviour)
        await asyncio.gather(implement_bar(), use_behaviours())
