import abc
import logging
import typing as t

import ubii.proto as ub


class IServiceConnection(abc.ABC):
    @abc.abstractmethod
    async def send(self, request: ub.ServiceRequest) -> ub.ServiceReply:
        """
        Send a ServiceRequest message, and receive a ServiceReply, according to the protobuf specifications.
        Can be implemented with any communication primitives the master node supports.

        :param request: ServiceRequest protobuf message (wrapper)
        """
        ...


class IServiceCall(ub.Service, metaclass=ub.ProtoMeta):
    """
    A callable version of a Service Wrapper that implements the respective service call
    """

    @abc.abstractmethod
    async def __call__(self, connection: IServiceConnection, **message) -> ub.ServiceReply: ...


class IServiceCallConverter(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def make_service_call(specs: ub.Service, **kwargs) -> IServiceCall:
        """
        Convert a Service message to IServiceCall object.

        :param specs: Service protobuf message (wrapper)
        """
        ...


class IServiceClient(abc.ABC):
    @property
    @abc.abstractmethod
    def services(self) -> t.Mapping[str, IServiceCall]:
        """
        A mapping of topics to service calls
        """
        ...


