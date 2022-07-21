import asyncio
import logging
import random
import typing as t

import pytest
import ubii.proto as ub

from ubii.framework.topics import DataConnection, TopicStore, BasicTopic, StreamSplitRoutine

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)


class MockConnection(DataConnection):
    """
    Mock data published to topics 'A' and 'B'
    """

    def __init__(self, max_items_send: int):
        self._max_items_send = max_items_send
        self._async_gen_coro = self._gen_data()

    async def send(self, data: ub.TopicData):
        raise NotImplementedError

    def _gen_record(self):
        for i in range(self._max_items_send):
            topic = 'A' if i % 2 == 0 else 'B'

            if i % 2 == 0:
                yield ub.TopicDataRecord(topic=topic, bool=i % 3 != 0)
            else:
                yield ub.TopicDataRecord(topic=topic, int32=i)

    async def _gen_data(self):
        for record in self._gen_record():
            yield ub.TopicData(topic_data_record=record)
            await asyncio.sleep(random.randrange(1, 3) * 0.1)

    def __anext__(self) -> t.Awaitable[ub.TopicData]:
        return self._async_gen_coro.__anext__()


class Data(t.NamedTuple):
    container: TopicStore
    coro: StreamSplitRoutine


@pytest.fixture
def make_connection(request):
    """
    Create a StreamSplitRoutine from a MockConnection and a container, return topics 'A' and 'B' as well as the routine
    """

    def generate(*, max_items_send: int):
        return MockConnection(max_items_send=max_items_send)

    yield generate


@pytest.fixture
def container():
    container = TopicStore(BasicTopic)
    yield container


@pytest.mark.parametrize('items', [20, 1, 99])
async def test_topics(items, make_connection, container):
    """
    Test if splitting to topics and callbacks work correctly
    """
    a, b = container['A'], container['B']
    a_received = []
    b_received = []

    a.register_callback(a_received.append)
    b.register_callback(b_received.append)

    await asyncio.create_task(
        StreamSplitRoutine(container=container, stream=make_connection(max_items_send=items))
    )

    assert len(a_received) == (items + 1) // 2
    assert len(b_received) == items - len(a_received)

    assert [record.bool for record in a_received] == list(map(lambda i: i % 3 != 0, range(len(a_received))))
    assert [record.int32 for record in b_received] == list(filter(lambda x: x % 2, range(items)))


@pytest.mark.parametrize('items', [15])
async def test_task_manager(make_connection, container, items):
    """
    Test if the task_manager (in charge of canceling the tasks) works properly
    """
    a, b = container['A'], container['B']
    a_received = []
    b_received = []

    # register tasks with task manager, tasks for topic 'a' should be automatically unregistered after async with block
    async with a.task_nursery:
        a.register_callback(a_received.append)
        b.register_callback(b_received.append)
        await asyncio.create_task(
            StreamSplitRoutine(container=container, stream=make_connection(max_items_send=items))
        )

    assert all(t.cancelled() for t in a.callback_tasks.values())  # should be cancelled
    assert not any(t.cancelled() for t in b.callback_tasks.values())  # should not be cancelled

    # check results to see if tasks ran correctly and were not cancelled prematurely
    assert [record.bool for record in a_received] == list(map(lambda i: i % 3 != 0, range(len(a_received))))
    assert [record.int32 for record in b_received] == list(filter(lambda x: x % 2, range(items)))

    # reset the topic to be reused for further testing
    a.callback_tasks.clear()
    a.task_nursery = type(a.task_nursery)(name=a.task_nursery.name, loop=a.task_nursery.loop)

    # register again ...
    async with a.task_nursery:
        a.register_callback(a_received.append)
        # ... but change task_manager
        # previously this was a feature of a topic, now we do it explicitly
        b.task_nursery.push_async_exit(a.task_nursery.pop_all())

    assert not any(t.cancelled() for t in b.callback_tasks.values())  # should not be cancelled
    assert not any(
        t.cancelled() for t in a.callback_tasks.values()
    )  # should not be cancelled since task_manager was changed before block was closed

    await b.task_nursery.aclose()  # should cancel all tasks (also for topic 'a')
    assert all(t.cancelled() for t in b.callback_tasks.values())
    assert all(t.cancelled() for t in a.callback_tasks.values())


@pytest.mark.parametrize('items', [15])
async def test_task_regex_topics(make_connection, container, items):
    all_regex = container['*']
    all_regex_received = []
    all_regex.register_callback(all_regex_received.append)

    await asyncio.create_task(
        StreamSplitRoutine(container=container, stream=make_connection(max_items_send=items))
    )

    assert len(all_regex_received) == items

    a_regex = container['A*']
    a_regex_received = []
    a_regex.register_callback(a_regex_received.append)

    await asyncio.create_task(
        StreamSplitRoutine(container=container, stream=make_connection(max_items_send=items))
    )

    assert len(all_regex_received) == 2 * items
    assert len(a_regex_received) == (items + 1) // 2
