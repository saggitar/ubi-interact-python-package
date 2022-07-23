import asyncio
import functools
import uuid

import pytest
import ubii.proto

import ubii.framework.client
import ubii.framework.processing
import ubii.framework.topics
import ubii.node.pytest


@pytest.fixture
def topic_data(request):
    return [ubii.proto.TopicData(topic_data_record=value) for value in request.param]


FakeMuxer = functools.partial(ubii.framework.topics.TopicMuxer, id=str(uuid.uuid4()))


class TestMuxer:
    @pytest.mark.parametrize('topic_data', [
        [{'topic': 'test/foo', 'bool': n % 2 == 0} for n in range(10)]
    ], indirect=True)
    async def test_muxer_basics(self, topic_data):
        topic_muxer = FakeMuxer(data_type='bool')
        for data in topic_data:
            await topic_muxer.records.set([data.topic_data_record])

        assert len(topic_muxer.records.value) == 1
        assert topic_muxer.records.value[0] == topic_data[-1].topic_data_record

    @pytest.mark.parametrize('topic_data', [
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
    async def test_identity_match(self, topic_data, topic, identity_match_pattern, identity):
        topic_muxer = FakeMuxer(data_type='bool', identity_match_pattern=identity_match_pattern)
        for data in topic_data:
            data.topic_data_record.topic = topic
            await topic_muxer.records.set([data.topic_data_record])

        assert len(topic_muxer.records.value) == 1
        record = topic_muxer.records.value[0]
        assert record.metadata()['identity'] == identity


class TestMuxerProcessing:
    processing_module = ubii.proto.ProcessingModule(
        name="muxer-processing-module",
        processing_mode={'trigger_on_input': {'min_delay_ms': 0,
                                              'all_inputs_need_update': True}},

        inputs=[
            {
                'internal_name': 'muxer_inputs',
                'message_format': 'int32'
            },
        ],
        outputs=[
            {
                'internal_name': 'demuxer_outputs',
                'message_format': 'int32'
            }
        ],
        on_processing_stringified='\n'.join((
            'def on_processing(self, context):',
            '    import ubii.proto',
            '    outputs = []',
            '    for record in context.inputs.muxer_inputs:',
            '        metadata = record.metadata()',
            '        if record.int32 % 2 == 0:',
            '            outputs += [{"int32": record.int32, "output_params": (metadata["identity"],)}]',
            '        else:',
            '            outputs += [{"int32": record.int32, "output_params": ("dummy-1234",)}]',
            '    context.outputs.demuxer_outputs = outputs',
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
                            'data_type': 'int32',
                            'topic_selector': '/muxer/*',
                            'identity_match_pattern': '(?:/muxer/([0-9a-z-]+))'
                        },
                        'input_name': processing_module.inputs[0].internal_name,
                    },
                ],
                'output_mappings': [
                    {
                        'topic_demux': {
                            'data_type': 'int32',
                            'output_topic_format': '/demuxer/{{#0}}',
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

    @pytest.mark.parametrize('topic_data', [
        [{'int32': n} for n in range(10)]
    ], indirect=True)
    async def test_muxer_processing(self, client, session_spec, module_spec, topic_data):
        await client
        await client.implements(ubii.framework.client.Sessions)
        await client[ubii.framework.client.Sessions].start_session(session_spec)

        topic, = await client[ubii.framework.client.Subscriptions].subscribe_regex('/demuxer/*')
        received = []
        topic.register_callback(received.append)

        for data in topic_data:
            data.topic_data_record.topic = f"/muxer/{client.id}"
            await client[ubii.framework.client.Publish].publish(data.topic_data_record)
            await asyncio.sleep(0.02)  # stagger inputs

        assert len(received) == 5
        assert all(record.topic == f"/demuxer/{client.id}" for record in received)
        assert all(value in [record.int32 for record in received] for value in [0, 2, 4, 6, 8])
        await client[ubii.framework.client.Sessions].stop_session(session_spec)
