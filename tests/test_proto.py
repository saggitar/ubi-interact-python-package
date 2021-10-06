import dataclasses
import json
from collections import namedtuple
from pathlib import Path

from typing import Dict
import pytest
from ubii_interact.util.proto import Translators, Translator


GENERATE_TEST_DATA = True


class TestTranslators:
    translators: Dict[str, Translator] = dataclasses.asdict(Translators)

    @dataclasses.dataclass
    class Data:
        translator: Translator
        test_data: dict
        test_data_file: Path


    @pytest.fixture
    def data(self, get_json_data, write_json_data, make_file, request):
        msg_type: str = request.param
        translator = self.translators.get(msg_type)
        test_data = get_json_data(msg_type)

        if not any(test_data.values()) and GENERATE_TEST_DATA:
            file = make_file(f"{msg_type.lower()}.json")
            message = translator.create()
            data = {
                'dict': translator.to_dict(message, including_default_value_fields=True),
                'bytes': message.SerializeToString(deterministic=True).decode('utf-8'),
                'json': translator.to_json(message, including_default_value_fields=True)
            }
            write_json_data(file, data)

        for path, data in test_data.items():
            yield self.Data(translator=translator, test_data=data, test_data_file=path)

    @pytest.mark.parametrize('data', translators, indirect=True)
    def test_from_bytes(self, data: Data, write_json_data):
        if not data.test_data:
            return

        converted = data.translator.from_dict(data.test_data['dict'])
        if GENERATE_TEST_DATA:
            data.test_data['bytes'] = converted.SerializeToString(deterministic=True).decode('utf-8')
            write_json_data(file=data.test_data_file, data=data.test_data)
        else:
            assert data == converted.SerializeToString()

    def test_from_json(self):
        assert False

    def test_from_dict(self):
        assert False

    def test_to_dict(self):
        assert False

    def test_to_json(self):
        assert False

    def test_convert_to_message(self):
        assert False

    def test_create(self):
        assert False

    def test_validate(self):
        assert False
