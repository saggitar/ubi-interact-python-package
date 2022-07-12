import asyncio
import logging
import uuid

import pytest
import ubii.proto

from test.test_processing import TestPy as _TestPy
from ubii.framework.client import RunProcessingModules, Publish, Subscriptions

WRITE_PERFORMANCE_DATA = True

pytestmark = pytest.mark.asyncio


class TestPerformance(_TestPy):
    test_module = ubii.proto.ProcessingModule(
        on_created_stringified='\n'.join((
            'def on_created(self, context):',
            '    context.frame_count = 0',
            '    context.delta_times = []',
            '',
        )),
        on_processing_stringified='\n'.join((
            'def on_processing(self, context):',
            '    context.frame_count += 1',
            '    if context.frame_count % 30 == 0:',
            '        context.frame_count = 0',
            '        context.delta_times.extend(context.scheduler.delta_times)',
        )),
        language=ubii.proto.ProcessingModule.Language.PY,
    )

    hertz_10 = ubii.proto.ProcessingModule(
        **type(test_module).to_dict(test_module),
        processing_mode={'frequency': {'hertz': 10}},
    )

    module_spec = [
        pytest.param((hertz_10,), id='10 hertz'),
    ]

    client_spec = [
        pytest.param((ubii.proto.Client(
            is_dedicated_processing_node=True,
            processing_modules=[ubii.proto.ProcessingModule(name='example-processing-module')]
        ),), id='processing_node')
    ]

    @pytest.fixture
    def running_pm(self, client, base_module, data_dir, request):
        assert client.implements(RunProcessingModules)
        pm: ubii.proto.ProcessingModule = {pm.name: pm for pm in client[RunProcessingModules].running_pms}.get(
            base_module.name)
        pm._protocol.context.delta_times = []
        yield pm
        delta_times = pm._protocol.context.delta_times[1:]  # first value is time before processing was triggered
        assert delta_times
        assert all(r > 0 for r in delta_times)
        assert pm.processing_mode.frequency

        avg_delta = sum(delta_times) / len(delta_times)
        max_delta = max(delta_times)

        def relative_error(value):
            delay = 1. / pm.processing_mode.frequency.hertz
            return abs(delay - value) / delay

        if WRITE_PERFORMANCE_DATA:
            with (data_dir / request.node.name).open('w') as f:
                f.write(f'delta_times\n')
                f.write('\n'.join(map('{:.5f}'.format, delta_times)))

        assert relative_error(avg_delta) < 0.05
        max_error = relative_error(max_delta)

        print(f"Avg delta time: {avg_delta} over {len(delta_times)} measurements "
              f"for target delay of {1. / pm.processing_mode.frequency.hertz}s (max error: {max_error})")

    @pytest.mark.parametrize('data', [False, True])
    @pytest.mark.parametrize('delay', [0.0005])
    @pytest.mark.parametrize('duration', [10])
    async def test_processing_module(self, client, base_session, running_pm, data, delay, duration):
        published_count = int(duration / delay)
        for _ in range(published_count):
            await asyncio.sleep(delay)
            await client[Publish].publish({'topic': base_session.client_bool, 'bool': data})

        print(f"Published {published_count} times {data}")


class TestPerf30Hz(TestPerformance):
    hertz_30 = ubii.proto.ProcessingModule(
        **type(TestPerformance.test_module).to_dict(TestPerformance.test_module),
        processing_mode={'frequency': {'hertz': 30}},
    )
    module_spec = [
        pytest.param((hertz_30,), id='30 hertz'),
    ]


class TestPerf60Hz(TestPerformance):
    hertz_60 = ubii.proto.ProcessingModule(
        **type(TestPerformance.test_module).to_dict(TestPerformance.test_module),
        processing_mode={'frequency': {'hertz': 60}},
    )
    module_spec = [
        pytest.param((hertz_60,), id='60 hertz'),
    ]


class TestPerf120Hz(TestPerformance):
    hertz_120 = ubii.proto.ProcessingModule(
        **type(TestPerformance.test_module).to_dict(TestPerformance.test_module),
        processing_mode={'frequency': {'hertz': 120}},
    )
    module_spec = [
        pytest.param((hertz_120,), id='120 hertz'),
    ]


class TestPublishReceivePerformance:
    websocket_in_log = logging.getLogger('ubii.node.connections.in.socket')
    websocket_out_log = logging.getLogger('ubii.node.connections.out.socket')

    @pytest.fixture
    def publish_count(self, request):
        yield request.param

    @pytest.fixture
    def receive_delay(self, request):
        yield request.param

    @pytest.fixture
    async def subscription(self, client, publish_count, receive_delay):
        await client
        received = []
        topic_id = str(uuid.uuid4())
        topics, tokens = await client[Subscriptions].subscribe_topic(topic_id).with_callback(received.append)
        yield topics[0]
        await asyncio.sleep(receive_delay)
        self.websocket_in_log.info('Removing Callback now!')
        assert await topics[0].unregister_callback(tokens[0], timeout=1)
        await client[Subscriptions].unsubscribe_topic(topics[0].pattern)
        assert len(received) == publish_count

    @pytest.mark.parametrize('data', [False])
    @pytest.mark.parametrize('delay', [0.0005, 0.001, 0.005, 0.01])
    @pytest.mark.parametrize('publish_count', [10], indirect=True)
    @pytest.mark.parametrize('receive_delay', [0.1], indirect=True)
    async def test_publish(self, client, subscription, delay, publish_count, data, receive_delay):
        """
        Publish data with small delay, test if all data is received in time
        """
        for _ in range(publish_count):
            await asyncio.sleep(delay)
            await client[Publish].publish({'topic': subscription.pattern, 'bool': data})

        self.websocket_out_log.info(f"Published {publish_count} times {data}")
