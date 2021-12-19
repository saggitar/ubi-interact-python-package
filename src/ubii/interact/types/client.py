import abc

from .services import IServiceClient
from .topics import ITopicClient


class IUbiiClient(IServiceClient, ITopicClient, abc.ABC):
    """
    The interface of a Ubi Interact Client.
    """
    pass
