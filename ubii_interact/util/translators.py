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
    def from_bytes(cls, obj, *args, **kwargs):
        p = cls.proto()
        p.ParseFromString(obj, *args, **kwargs)
        return p

    @classmethod
    def from_json(cls, obj, *args, **kwargs):
        return Parse(obj, cls.proto(), *args, **kwargs)

    @classmethod
    def from_dict(cls, obj, *args, **kwargs):
        """
        We need to accept dictionaries with CamelCase keys (because the
        REST Server replies with accordingly formatted json) as well as
        keys that are the exact proto field names.

        We also want to allow dictionaries of "mixed" format,
        containing initialized protobuf objects as well as message representations
        using plain python types.

        Creating proto messages with keyword attributes only works with
        the exact proto field names, and using ParseDict does not support
        nesting already initialized protobuf objects (only plain python),
        so we combine both methods to get a nice API.


        :param obj: a dictionary
        :param args: possible positional arguments for ParseDict
        :param kwargs: keyword arguments for ParseDict or the protobuf message __init__ method.
        :return: valid protobuf message
        """
        try:
            obj = cls.proto(**obj, **kwargs)
            if args:
                log.debug(f"Ignored arguments {', '.join(args)} when parsing {obj}")
            return obj
        except ValueError:
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
            if isinstance(obj, bytes):
                return cls.from_bytes(obj, *args, **kwargs)
            if isinstance(obj, dict):
                return cls.from_dict(obj, *args, **kwargs)
            elif isinstance(obj, str):
                return cls.from_json(obj, *args, **kwargs)
            else:
                raise ParseError(f"Type {type(obj).__name__} is not supported. Use dictionaries or json formatted strings.")
        except ParseError as e:
            log.exception(e)
            raise

    @classmethod
    def create(cls, *args, **kwargs):
        if not args:
            return cls.from_dict(kwargs)
        else:
            return cls.proto(*args, **kwargs)


def generate_type(datatype: str) -> type:
    # The .proto files declare a package name 'ubii', but this is not reflected in the python package.
    # Instead the package is named proto, since the name is defined by directory structure, see
    # https://developers.google.com/protocol-buffers/docs/reference/python-generated#package
    if not datatype.startswith('ubii.'):
        log.debug(f"Type {datatype} is not a protobuf type.")
    else:
        return ProtoMessage.get_type(packages.import_proto(datatype))


ProtoMessages: Dict[str, ProtoMessage] = apply(generate_type, __constants__['MSG_TYPES'])

