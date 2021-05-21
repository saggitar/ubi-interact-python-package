import json
import logging
from json import JSONEncoder
from typing import Dict, Union, Any
from ..util import packages, apply, constants as __constants__
from google.protobuf.json_format import Parse, MessageToJson, MessageToDict, ParseDict, ParseError

log = logging.getLogger(__name__)

def serialize(*args, **kwargs):
    result = json.dumps(*args, cls=ProtoMessage.ProtoEncoder, **kwargs)
    return result


class ProtoMessage:
    PREFIX = "Translator"
    __types__ = {}
    proto = None

    class ProtoEncoder(JSONEncoder):
        def default(self, o):
            try:
                return ProtoMessage.to_dict(o)
            except ParseError:
                return JSONEncoder.default(self, o)

    @classmethod
    def get_type(cls, base):
        name = f"{cls.PREFIX}{base.__name__}"
        return cls.__types__.setdefault(name, type(name, (ProtoMessage,), {}, proto=base))

    def __init_subclass__(cls, /, proto, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.proto = proto

    def __new__(cls, *args, **kwargs):
        raise NotImplementedError(f"Instantiating objects of type {cls.__name__} is not supported. Use"
                                  f" {cls.__name__ + '.' + cls.create.__name__} to create messages.")

    @classmethod
    def from_json(cls, obj, *args, **kwargs):
        return Parse(obj, cls.proto(), *args, **kwargs)

    @classmethod
    def from_dict(cls, obj, *args, **kwargs):
        return ParseDict(obj, cls.proto(), *args, **kwargs)

    @classmethod
    def to_dict(cls, message, use_integers_for_enums=True, including_default_value_fields=True, **kwargs):
        return MessageToDict(message,
                             use_integers_for_enums=use_integers_for_enums,
                             including_default_value_fields=including_default_value_fields,
                             **kwargs)

    @classmethod
    def to_json(cls, message, use_integers_for_enums=True, including_default_value_fields=True, **kwargs):
        return MessageToJson(message,
                             use_integers_for_enums=use_integers_for_enums,
                             including_default_value_fields=including_default_value_fields,
                             **kwargs)

    @classmethod
    def convert_to_message(cls, obj: Union[str, Dict[str, Any]], *args, **kwargs):
        """
        :param obj: dictionary or json formatted string
        """
        if not cls.proto:
            raise NotImplementedError
        try:
            if isinstance(obj, dict):
                return cls.from_dict(obj, *args, **kwargs)
            elif isinstance(obj, str):
                return cls.from_json(obj, *args, **kwargs)
            else:
                raise ParseError(f"Type {type(obj).__name__} is not supported. Use dictionaries or json formatted strings.")
        except ParseError as e:
            log.error(e)
            raise

    @classmethod
    def create(cls, *args, **kwargs):
        if not args:
            kwargs = cls.verify(kwargs, preserving_proto_field_name=True)

        return cls.proto(*args, **kwargs)

    @classmethod
    def verify(cls, obj: Union[str, Dict[str, Any]], **kwargs):
        """
        MessageToDict produces CamelCase keys, which is the only
        way that's supported by the server. We supply this helper function to validate dictionaries by
        converting to message and back.
        Since sending json is only needed by the REST Client, processing time is no issue.
        """
        return cls.to_dict(cls.convert_to_message(obj), including_default_value_fields=True, **kwargs)


def generate_type(datatype: str) -> type:
    # The .proto files declare a package name 'ubii', but this is not reflected in the python package.
    # Instead the package is named proto, since the name is defined by directory structure, see
    # https://developers.google.com/protocol-buffers/docs/reference/python-generated#package
    if not datatype.startswith('ubii.'):
        log.info(f"Type {datatype} is not a protobuf type.")
    else:
        return ProtoMessage.get_type(packages.import_proto(datatype))


protomessages: Dict[str, ProtoMessage] = apply(generate_type, __constants__['MSG_TYPES'])
