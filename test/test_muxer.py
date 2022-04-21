import asyncio

import pytest

import ubii.framework.client
import ubii.framework.processing
import ubii.framework.topics
import ubii.proto


@pytest.fixture
def test_data(request):
    return [ubii.proto.TopicData(topic_data_record=value) for value in request.param]


class TestMuxer:
    @pytest.mark.parametrize('test_data', [
        [{'topic': 'test/foo', 'bool': n % 2 == 0} for n in range(10)]
    ], indirect=True)
    async def test_muxer_basics(self, test_data):
        topic_muxer = ubii.framework.topics.TopicMuxer()
        for data in test_data:
            await topic_muxer.on_record(data.topic_data_record)

        assert len(topic_muxer.records.value) == 1
        assert topic_muxer.records.value[0] == test_data[0].topic_data_record

    @pytest.mark.parametrize('test_data', [
        pytest.param([{'bool': n % 2 == 0} for n in range(10)], id='alternating_bools')
    ], indirect=True)
    @pytest.mark.parametrize(
        'topic, identity_match_pattern, identity',
        [
            ('test/foo-1234', r'test/(.*)', 'foo-1234',),
            ('test/foo/bar', r'.+', 'test/foo/bar'),
            ('test/foo-1234', r'\d+', '1234')
        ]
    )
    async def test_identity_match(self, test_data, topic, identity_match_pattern, identity):
        topic_muxer = ubii.framework.topics.TopicMuxer(identity_match_pattern=identity_match_pattern)
        for data in test_data:
            data.topic_data_record.topic = topic
            await topic_muxer.on_record(data.topic_data_record)

        assert len(topic_muxer.records.value) == 1
        record = topic_muxer.records.value[0]
        assert topic_muxer.identity(record) == identity


class TestMuxerProcessing:
    processing_module = ubii.proto.ProcessingModule(
        name="muxer-processing-module",
        processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                              'all_inputs_need_update': True}},

        inputs=[
            {
                'internal_name': 'value',
                'message_format': 'bool'
            },
        ],
        outputs=[
            {
                'internal_name': 'value',
                'message_format': 'bool'
            }
        ],
        on_processing_stringified='\n'.join((
            'def on_processing(self, context):',
            '   for record in context.inputs.muxer_inputs:',
            '       identity = context.muxer.identity(record)',
            '       context.demuxer.set_output_params(record, identity)',
            '       context.outputs.value = context.inputs.value',
        )),
        language=ubii.proto.ProcessingModule.Language.PY,
    )

    session = ubii.proto.Session(
        name="Muxer Session",
        processing_modules=[processing_module],
        io_mappings=[
            {
                'processing_module_name': processing_module.name,
                'input_mappings': [
                    {
                        'topic_mux': {
                            'data_type': 'bool',
                            'topic_selector': '/muxer/*',
                            'identity_match_pattern': '[0-9a-z-]+'
                        },
                        'input_name': processing_module.inputs[0].internal_name,
                    },
                ],
                'output_mappings': [
                    {
                        'topic_demux': {
                            'data_type': 'bool',
                            'output_topic_format': '/muxer/{{#0}}',
                        },
                        'output_name': processing_module.outputs[0].internal_name,
                    }
                ]
            },
        ]
    )

    client = ubii.proto.Client(
        name='Muxer Client',
        processing_modules=[processing_module],
        is_dedicated_processing_node=True
    )

    session_spec = [(session,)]
    module_spec = [(processing_module,)]
    client_spec = [(client,)]

    @pytest.mark.parametrize('test_data', [
        [{'bool': n % 2 == 0} for n in range(10)]
    ], indirect=True)
    async def test_muxer_processing(self, client_spec, session_spec, module_spec, start_session, test_data):
        client = await client_spec
        session = await start_session(session_spec)
        output_topic = session.io_mappings[0].output_mappings[0].topic
        output, = await client[ubii.framework.client.Subscriptions].subscribe_topic(output_topic)

        retrieved = []

        def test(record):
            print()

        output.register_callback(test)

        for data in test_data:
            data.topic_data_record.topic = f"/muxer/input"
            await client[ubii.framework.client.Publish].publish(data.topic_data_record)

        await asyncio.sleep(3)
        print()
