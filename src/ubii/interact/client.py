from __future__ import annotations

import asyncio
import dataclasses
import typing as t
import warnings
from contextlib import asynccontextmanager
from functools import partial
from itertools import chain
from warnings import warn

import ubii.proto as ub
from ._typing import SimpleCoroutine
from ._util import ProtoRegistry
from .logging import ProtoFormatMixin, debug
from .protocol import UbiiProtocol
from .services import DefaultServiceMap
from .topics import Topic

__protobuf__ = ub.__protobuf__


@dataclasses.dataclass
class BasicBehaviour:
    """
    Behavior of the client that needs to be injected
    """
    _subscribe_type = t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[t.Tuple[Topic, ...]]]
    _unsubscribe_type = t.Callable[[t.Tuple[str, ...]], SimpleCoroutine[bool]]

    register: t.Any | None = None
    deregister: t.Any | None = None
    subscribe_regex: _subscribe_type | None = None
    subscribe_topic: _subscribe_type | None = None
    unsubscribe_regex: _unsubscribe_type | None = None
    unsubscribe_topic: _unsubscribe_type | None = None
    publish: t.Callable[[t.Tuple[ub.TopicDataRecord, ...]], SimpleCoroutine[None]] | None = None
    services: DefaultServiceMap | None = None


@dataclasses.dataclass
class DeviceManager:
    """
    Behavior to register and deregister Devices (optional)
    """
    register_device: t.Callable[[ub.Device], SimpleCoroutine[ub.Device]] | None = None
    deregister_device: t.Callable[[ub.Device], SimpleCoroutine[None]] | None = None


@dataclasses.dataclass
class ProcessingModuleManager:
    """
    Behavior to update and run processing modules
    """
    processing_module_manager: bool = False


class UbiiClient(ub.Client, t.Awaitable['UbiiClient'], ProtoFormatMixin, metaclass=ProtoRegistry):
    """
    A Client is a wrapper around a ``Client`` proto message.
    It can perform the following additional features:

    *   make ``ServiceCall``[s] (by accessing the right Service for your task by topic,
        and calling it with the right kind of data - see https://github.com/SandroWeber/ubi-interact/wiki/Requests for
        more documentation on default topics for services and expected data, and the documentation for ``ServiceMap``)

    *   subscribe to topics (or topic patterns) at the master node. This process involves making the right service call
        and then creating a internal representation of the topic to add callbacks and forward received data.
        Because of this complexity you should not subscribe to topics via a simple ServiceCall, and instead use the
        dedicated methods
        TODO: add methods
        Make sure to use the ``_regex`` version of a method when you subscribe to a wildcard pattern see
        TODO: add link

    *   publish data on topics. This requires a TopicData message or a compatible dictionary (see documentation of the
        message formats) to be passed to the ``publish`` method.

    *   run processing modules: This is typically how you tell the client to do complex things. Processing modules
        need to be registered at your client by either passing them during initialization, or registering them
        afterwards.
        TODO implement and document

    *   run a ``UbiiProtocol``. A ``UbiiProtocol`` implementation defines several callbacks to be called during the
        lifetime of a client. For more detail see the ``UbiiProtocol`` documentation and the documentation of the
        default protocol to understand the setup and teardown of a client

    """

    # key for registry
    __unique_key_attr__ = 'id'

    def __init__(self, *,
                 protocol: UbiiProtocol,
                 required_behaviours: t.Tuple[t.Type, ...] = (BasicBehaviour,),
                 optional_behaviours: t.Tuple[t.Type, ...] = (DeviceManager, ProcessingModuleManager),
                 **kwargs):
        super().__init__(**kwargs)
        self.name: str
        if not self.name:
            self.name = f"{self.__class__.__name__}"

        self._behaviours = {kls: kls() for kls in chain(required_behaviours or (), optional_behaviours or ())}
        if not all(dataclasses.is_dataclass(b) for b in self._behaviours):
            raise ValueError(f"Only dataclasses can be passed as behaviours")

        self._required_behaviours = required_behaviours or ()

        self._change_specs = asyncio.Condition()
        self._protocol = protocol
        self._implemented = None
        self.__ctx: t.AsyncContextManager = self.__with_running_protocol()
        self._init = self.protocol.create_task(self._initialize())

    @property
    def change_specs(self):
        return self._change_specs

    async def implement(self, behaviour: t.Mapping | None = None, **kwargs):
        """
        Set the implementation of one of the clients key behaviours, or update it.
        Searches the dataclass or keyword in the supplied behaviours and replaces the field value[s] from
        the supplied argument

        :param behaviour: opional dataclass instance of one of the required or optional behaviour classes
        :param kwargs: keyword arguments for fields in one of the required or optional behaviour classes
        """
        if behaviour and not dataclasses.is_dataclass(behaviour):
            raise ValueError(f"behaviour needs to be a dataclass instance (one of required or optional behaviours)")

        behaviours_matching_type = {
            kls: instance for kls, instance in self._behaviours.items() if isinstance(instance, type(behaviour))
        }

        if behaviour and not behaviours_matching_type:
            raise ValueError(f"no behaviours from {self} matched argument {behaviour}")

        for kls, instance in behaviours_matching_type.items():
            self._behaviours[kls] = dataclasses.replace(instance, **(behaviour or {}))

        matching_kwarg_types = {
            name: [b for b in self._behaviours if name in [field.name for field in dataclasses.fields(b)]]
            for name in kwargs
        }

        _missing = [k for k in matching_kwarg_types if not matching_kwarg_types[k]]
        if _missing:
            raise ValueError(f"no fields from behaviours of {self} match keywords {', '.join(map(str, _missing))}")

        for name, matching in matching_kwarg_types.items():
            for kls in matching:
                self._behaviours[kls] = dataclasses.replace(self._behaviours[kls], **{name: kwargs[name]})

        # the behaviour property needs to be recomputed, so it needs to be reset.
        # this is typically done with a custom deleter on the property, but the proto-plus parent class
        # intercepts normal attribute deletion, which prevents the deleter of the descriptor to run normally.
        # workarounds:
        #   a) overwrite __delattr__ and handle deleting there (intercept the interception :S)
        #   b) set the base attribute of the computed ``behaviour`` attribute to None manually (bad idea)
        #   c) call the deleter of the property manually <- see below
        type(self).behaviour.fdel(self)  # type: ignore
        async with self._change_specs:
            self._change_specs.notify_all()

    async def _initialize(self):
        await self.implements(*self._required_behaviours)
        return self

    def __await__(self):
        with warnings.catch_warnings():
            # this might not be the first call to run() but we know this, so it's ok.
            warnings.simplefilter('ignore', UserWarning)
            self.protocol.run()

        return self._init.__await__()

    @asynccontextmanager
    async def __with_running_protocol(self):
        async with self.protocol:
            client = await self
            yield client

    def __aenter__(self):
        return self.__ctx.__aenter__()

    def __aexit__(self, *exc_info):
        return self.__ctx.__aexit__(*exc_info)

    @property
    def protocol(self) -> UbiiProtocol:
        return self._protocol

    @property
    def behaviour(self) -> t.Mapping[str, t.Any]:
        """
        Returns a mapping of implemented functionalities from the required and optional behaviours.
        Keys are the names that can be used to access the functionality on the client
        """
        if self._implemented is None:
            self._implemented = {
                field.name: getattr(b, field.name) for b in self._behaviours.values() for field in dataclasses.fields(b)
            }
        return self._implemented

    @behaviour.deleter
    def behaviour(self):
        self._implemented = None

    def does_implement(self, *behaviours: t.Type | str) -> bool:
        """
        Checks if all fields from the dataclasses passed as arguments are present in ``self.behaviour``

        :param behaviours: dataclass types or instances
        """
        fields = chain.from_iterable(
            dataclasses.fields(b) for b in behaviours if dataclasses.is_dataclass(b)  # noqa
        )
        attributes = [f.name for f in fields] + [b for b in behaviours if isinstance(b, str)]
        exists = [hasattr(self, attr) for attr in attributes]
        return all(exists)

    async def implements(self, *behaviours: t.Type | str):
        """
        Wait until the client implements the behaviours

        :param behaviours:
        :type behaviours:
        :return:
        :rtype:
        """
        async with self._change_specs:
            await self._change_specs.wait_for(partial(self.does_implement, *behaviours))

    def _get_debug_info(self, missing_key):
        from ._util import similar
        matches = similar(
            choices=map(lambda f: f.name, chain.from_iterable(dataclasses.fields(b) for b in self._behaviours)),
            item=missing_key
        )

        info = f"No attribute {missing_key} found in {self.__class__.__name__}."
        if matches:
            info += f" Best match[es] in behaviour fields: {', '.join(matches)}"
            warn(info)
        return info

    def __getattr__(self, item):
        """
        Try protobuf fields first, then try behavior fields
        """
        try:
            return super().__getattr__(item)
        except AttributeError as e:
            if item in self.behaviour:
                value = self.behaviour[item]
                if value is None:
                    raise AttributeError(f"Attribute {item} not implemented in {self}")
                else:
                    return value
            else:
                info = self._get_debug_info(missing_key=item)
                if debug():
                    raise AttributeError(info) from e
                else:
                    raise
