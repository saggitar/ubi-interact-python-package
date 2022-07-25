import asyncio
import logging
import types
import uuid

import pytest
import ubii.proto as ub

from ubii.framework import processing
from ubii.framework.client import RunProcessingModules, Subscriptions, Publish, Sessions

pytestmark = pytest.mark.asyncio

__protobuf__ = ub.__protobuf__

log = logging.getLogger(__name__)

MODULE_NAME = "example-processing-module"
PARAM_NAME = 'test'

js_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        "function processingCallback(deltaTime, inputs) {",
        "  let outputs = {};",
        f"  outputs.{PARAM_NAME} = !inputs.{PARAM_NAME};",
        "  return {outputs};",
        "}",
    )),
    language=ub.ProcessingModule.Language.JS,
)

py_on_processing_stringified = ub.ProcessingModule(
    on_processing_stringified='\n'.join((
        'def on_processing(self, context):',
        '    import logging',
        '    import ubii.proto',
        '    log = logging.getLogger("ubii.interact.protocol")',
        '    log.info(f"------- delta_time: {context.delta_time} ---------")',
        f'    input_record = context.inputs.{PARAM_NAME}',
        f'    context.outputs.{PARAM_NAME} = ubii.proto.TopicDataRecord(bool=not input_record.bool)',
    )),
    language=ub.ProcessingModule.Language.PY,
)

py_on_created_stringified = ub.ProcessingModule(
    on_created_stringified='\n'.join((
        'def on_created(self, context):',
        '    import logging',
        '    log = logging.getLogger("ubii.interact.protocol")',
        '    log.info(f"------- TEST: {context} ---------")',
        '',
    )),
    language=ub.ProcessingModule.Language.PY,
)


class Processing:
    @pytest.fixture(scope='class')
    def base_module(self):
        processing_module = ub.ProcessingModule(
            name=MODULE_NAME,
            processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                                  'all_inputs_need_update': True}},

            on_processing_stringified="",
            inputs=[
                {
                    'internal_name': PARAM_NAME,
                    'message_format': 'bool'
                },
            ],
            outputs=[
                {
                    'internal_name': PARAM_NAME,
                    'message_format': 'bool'
                }
            ],
        )
        return processing_module

    @pytest.fixture(scope='class')
    async def base_session(self, client) -> ub.Session:
        await client
        module = client.processing_modules[0]
        input_topic = f"{client.id}/test_input"
        output_topic = f"{client.id}/test_output"
        io_mappings = [
            {
                'processing_module_name': module.name,
                'input_mappings': [
                    {
                        'input_name': module.inputs[0].internal_name,
                        'topic': input_topic
                    },
                ],
                'output_mappings': [
                    {
                        'output_name': module.outputs[0].internal_name,
                        'topic': output_topic
                    }
                ]
            },
        ]

        session = ub.Session(name=str(uuid.uuid4()),
                             processing_modules=[module],
                             io_mappings=io_mappings)

        yield session

    @pytest.fixture
    def pm_startup(self):
        pass

    @pytest.fixture
    async def test_value(self, pm_startup, client, base_session: ub.Session):
        topic, = await client[Subscriptions].subscribe_topic(base_session.io_mappings[0].output_mappings[0].topic)
        yield topic.buffer

    @pytest.mark.parametrize('data', [False, True])
    @pytest.mark.parametrize('delay', [1, 0.5, 0.2, 0.1])
    @pytest.mark.parametrize('timeout', [0.4])
    async def test_processing_module(self, client, test_value, base_session: ub.Session, delay, timeout, data):
        await asyncio.sleep(delay)
        await client[Publish].publish({'topic': base_session.io_mappings[0].input_mappings[0].topic, 'bool': data})
        record = await asyncio.wait_for(test_value.get(), timeout=timeout)

        assert isinstance(record.bool, bool)
        assert record.bool != data


class TestPy(Processing):
    module_spec = [
        pytest.param((py_on_processing_stringified, py_on_created_stringified), id='python')
    ]

    client_spec = [
        pytest.param((ub.Client(
            is_dedicated_processing_node=True,
            processing_modules=[ub.ProcessingModule(name=MODULE_NAME)]
        ),), id='processing_node')
    ]

    @pytest.fixture
    async def pm_startup(self, client, module_spec, base_session):
        await client.implements(RunProcessingModules)
        await client.implements(Sessions)
        started = await client[Sessions].start_session(base_session)

        pm = await client[RunProcessingModules].get_module_instance(
            module_spec.name, module_spec.Status.CREATED
        )
        yield pm

        await client[Sessions].stop_session(started)


@pytest.mark.xfail(reason="Broker bug")
class TestJS(Processing):
    module_spec = [
        pytest.param((js_on_processing_stringified,), id='js')
    ]

    client_spec = [
        pytest.param((ub.Client(is_dedicated_processing_node=False, processing_modules=None),), id='server_processing')
    ]


class TestLateInitModules(TestPy):
    class FakeModule(processing.ProcessingRoutine):
        """
        Test Module
        """

        def __init__(self, context, **kwargs):
            super().__init__(**kwargs)
            self.name = MODULE_NAME
            constants: ub.Constants = context.constants
            self.tags = ['pytest']
            self.description = 'Test Module'

            self.inputs = [
                {
                    'internal_name': 'test',
                    'message_format': constants.MSG_TYPES.DATASTRUCTURE_BOOL
                },
            ]

            self.outputs = [
                {
                    'internal_name': 'test',
                    'message_format': constants.MSG_TYPES.DATASTRUCTURE_BOOL
                },
            ]

            self.processing_mode = {
                'frequency': {
                    'hertz': 20
                }
            }
            self._context = context

        def on_processing(self, context: types.SimpleNamespace) -> None:
            input_value: ub.TopicDataRecord = getattr(context.inputs, PARAM_NAME)
            if input_value:
                setattr(context.outputs, PARAM_NAME, ub.TopicDataRecord(bool=not input_value.bool))

    late_init_module_spec = [
        pytest.param(FakeModule, id=MODULE_NAME)
    ]

    module_spec = []

    @pytest.mark.parametrize('data', [False, True])
    @pytest.mark.parametrize('timeout', [0.4])
    @pytest.mark.parametrize('run_time', [4])
    @pytest.mark.parametrize('allowed_relative_error', [0.9])
    async def test_processing_module(self,
                                     client,
                                     data,
                                     base_session: ub.Session,
                                     timeout,
                                     run_time,
                                     pm_startup,
                                     allowed_relative_error):
        received = []
        input_topic = base_session.io_mappings[0].input_mappings[0].topic
        output_topic = base_session.io_mappings[0].output_mappings[0].topic

        await client[Publish].publish({'topic': input_topic, 'bool': data})
        await asyncio.sleep(0.2)  # we wait until the broker processed the publishing
        topics, tokens = await client[Subscriptions].subscribe_topic(output_topic).with_callback(received.append)
        # if you subscribe to the same topic as before, you will immediately get the last value, therefore
        # we publish before we subscribe!

        await asyncio.sleep(run_time)
        await topics[0].unregister_callback(tokens[0], timeout=timeout)
        await client[Subscriptions].unsubscribe_topic(output_topic)

        assert all('bool' in r and r.bool != data for r in received), received
        tested_module: ub.ProcessingModule = base_session.processing_modules[0]

        max_computations = tested_module.processing_mode.frequency.hertz * run_time
        assert len(received) / max_computations >= allowed_relative_error
