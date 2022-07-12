import asyncio

import pytest

from ubii.framework.client import Subscriptions, Publish

pytestmark = pytest.mark.asyncio


class TestSubscriptions:

    @pytest.fixture(scope='class', autouse=True)
    async def startup(self, client):
        await client
        assert client.implements(Subscriptions, Publish)

    @pytest.mark.parametrize('test_params', ({'a': 1, 'b': 2, 'c': 3},))
    async def test_basic_subscriptions(self, client, test_params):
        topics = await client[Subscriptions].subscribe_topic(*(f"{client.id}/{name}" for name in test_params))

        for topic in topics:
            _, suffix = topic.pattern.rsplit('/', maxsplit=1)
            await client[Publish].publish({'topic': topic.pattern, 'int32': test_params[suffix]})

        received = await asyncio.gather(*[top.buffer.get(predicate=lambda v: v is not None) for top in topics])
        for received, sent in zip(received, test_params.values()):
            assert received.int32 == sent

    @pytest.mark.parametrize('pattern, value', [('foo*', 1), ])
    async def test_regex_subscriptions(self, client, pattern, value):
        topic, = await client[Subscriptions].subscribe_regex(f"{client.id}/{pattern}")

        await client[Publish].publish({'topic': topic.pattern[:-1] + 'bar', 'int32': value})
        received = await topic.buffer.get(predicate=lambda v: v is not None)

        assert received.int32 == value
