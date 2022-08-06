import asyncio
import logging
import uuid

import pandas as pd
import pytest
import ubii.proto as ub

from test.test_processing import TestPy as _TestPy
from ubii.framework.client import RunProcessingModules, Publish, Subscriptions, Sessions
from ubii.node.pytest import params

test_module = ub.ProcessingModule(
    on_created_stringified='\n'.join((
        'def on_created(self, context):',
        '    context.schedule_times = []',
        '    context.exec_times = []',
        '',
    )),
    on_processing_stringified='\n'.join((
        'def on_processing(self, context):',
        '    if context.scheduler.schedule_delta_times:',
        '        context.schedule_times.append(context.scheduler.schedule_delta_times[-1])',
        '    if context.scheduler.exec_delta_times:',
        '        context.exec_times.append(context.scheduler.exec_delta_times[-1])',
    )),
    language=ub.ProcessingModule.Language.PY,
)


def module(hertz):
    modified = ub.ProcessingModule(
        **type(test_module).to_dict(test_module),
        processing_mode={'frequency': {'hertz': hertz}},
    )
    return pytest.param((modified,), id=f'{hertz}hz')


def get_err_specs(*values):
    return {'count,mean,std,allowed_slow_frames': pytest.param(*values, id=f"err{'_'.join(map(str, values))}")}


class TestPerformance(_TestPy):
    module_spec = [module(h) for h in [200, 120, 60, 10]]  # overwrite class level module spec from TestPy

    module_spec_params = {
        '10hz': get_err_specs(0.15, 0.05, 0.05, 0),
        '60hz': get_err_specs(0.15, 0.1, 0.8, 0),
        '120hz': get_err_specs(0.15, 0.15, 3.5, 0),
        '200hz': get_err_specs(0.15, 0.2, 8.0, 0.01),
    }

    @pytest.fixture(scope='class')
    def count(self, request):
        """
        allowed relative error of counts
        """
        return request.param

    @pytest.fixture(scope='class')
    def mean(self, request):
        """
        allowed relative error of mean to target frequency
        """
        return request.param

    @pytest.fixture(scope='class')
    def std(self, request):
        """
        allowed std of average frequencies for 1s
        """
        return request.param

    @pytest.fixture(scope='class')
    def allowed_slow_frames(self, request):
        """
        Allowed relative number of frames where the execution time is larger than the delay
        associated with the target frequency
        """
        return request.param

    @pytest.fixture
    async def pm_startup(self,
                         client,
                         base_session,
                         module_spec,
                         reset_and_start_client,
                         caplog,
                         task_clean_frequency,
                         adjust_asyncio_delay):
        """
        Start the processing module, set the task clean frequency of the scheduler, wait until it is processing
        """
        caplog.set_level(logging.WARNING)
        await client.implements(RunProcessingModules, Sessions)

        started_session = await client[Sessions].start_session(base_session)
        pm = await client[RunProcessingModules].get_module_instance(module_spec.name)

        async with pm.change_specs:
            await pm.change_specs.wait_for(lambda: pm.status == pm.Status.CREATED)

        pm.protocol.context.scheduler.task_clean_frequency = task_clean_frequency

        if not adjust_asyncio_delay:
            pm.protocol.context.scheduler.timing_thresholds = ()

        async with pm.change_specs:
            await pm.change_specs.wait_for(lambda: pm.status == pm.Status.PROCESSING)

        yield pm

        await client[Sessions].stop_session(started_session)

    @pytest.mark.parametrize('duration', params('duration', 10))
    @pytest.mark.parametrize('task_clean_frequency', params('task', 1, 10, 30))
    @pytest.mark.parametrize('adjust_asyncio_delay', params('adjust_delay', True, False))
    @pytest.mark.parametrize('configure_logging', [{'version': 1}], indirect=True)
    async def test_processing_module(self,
                                     client,
                                     configure_logging,
                                     module_spec,
                                     base_session: ub.Session,
                                     pm_startup,
                                     duration,
                                     count,
                                     mean,
                                     std,
                                     test_data,
                                     task_clean_frequency,
                                     allowed_slow_frames,
                                     adjust_asyncio_delay,
                                     request):
        await asyncio.sleep(0.5)
        await client[Publish].publish({'topic': base_session.io_mappings[0].input_mappings[0].topic, 'bool': True})
        await asyncio.sleep(duration)

        # stop the processing module
        await type(pm_startup).halt(pm_startup)
        await type(pm_startup).stop(pm_startup)

        # evaluate performance
        target_freq = pm_startup.processing_mode.frequency.hertz
        target_count = duration * target_freq
        schedule_times = pd.Series(pm_startup.protocol.context.schedule_times[1:])
        exec_times = pd.Series(pm_startup.protocol.context.exec_times[5:])

        stats = (1. / schedule_times).describe()
        rolled_stats = (1. / schedule_times.rolling(target_freq // 2, min_periods=0).mean()).describe()

        # first write performance data even for failed tests, disable by setting
        # write_test_references=False in pytest.ini
        interesting_values = {
            'hz': target_freq,
            'task_clean_frequency': task_clean_frequency,
            'adjust_delay': adjust_asyncio_delay
        }
        with test_data.write('_'.join(f"{k}-{v}" for k, v in interesting_values.items())) as f:
            df = pd.concat({'raw': stats, 'rolling': rolled_stats}, axis=1)
            df.to_csv(f, float_format='%.4f')

        # check if the performance is ok
        assert (exec_times > (1. / target_freq)).sum() / len(exec_times) <= allowed_slow_frames
        assert not schedule_times.empty
        assert (schedule_times > 0).all()
        assert pm_startup.processing_mode.frequency
        assert abs(target_freq - rolled_stats['mean']) / target_freq < mean
        assert rolled_stats['std'] < std
        assert abs(target_count - rolled_stats['count']) / target_count < count
        print(rolled_stats)


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
    @pytest.mark.parametrize('delay', [
        pytest.param(0.0005, marks=pytest.mark.xfail(reason="Faster than communication channel")),
        0.001,
        0.005,
        0.01
    ])
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
