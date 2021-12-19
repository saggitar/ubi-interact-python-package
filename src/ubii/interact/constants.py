import os
import socket
import typing as t

import ubii.proto as ub

__protobuf__ = ub.__protobuf__

UBII_URL_ENV = 'UBII_SERVICE_URL'

_default_constants = ub.Constants()
_default_constants.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG = '/services/server_configuration'


class NodeConfig(t.NamedTuple):
    """
    Config options for the Ubi interact node.

    CONSTANTS are needed for all service calls, and typically provided by the master node.
    To get the config the defaults include the topic for the service_configuration service.

    SERVER includes all meta information about the master node (ip address, ports, and so on.)
    Currently the Server message contains a constants_json field which should be parsed as a ub.Constants message
    and updated in your config whenever the Server is updated. (At some point the master node might start sending actual
    proto messages instead of just json)

    LOCAL_IP is used by the REST Connection (and other service connections) for CORS headers.

    DEFAULT_SERVICE_URL is needed to make the first service request (server_configuration)
    before anything else is known. By default it's provided by a environment variable (see documentation of
    UBII_URL_ENV in this module)
    """
    CONSTANTS: ub.Constants = ub.Constants(mapping=_default_constants),
    SERVER: ub.Server = ub.Server()
    DEFAULT_SERVICE_URL: str = os.environ.get(UBII_URL_ENV),
    LOCAL_IP: str = socket.gethostbyname(socket.gethostname()),


# shared config
GLOBAL_CONFIG = NodeConfig(CONSTANTS=_default_constants)


class ConnectionConfig(t.NamedTuple):
    """
    Config options (and default values) shared between connections.
    The default values are taken from the global config, but changes are not applied (consider a ConnectionConfig as
    a read only data container with sensible defaults)
    """
    server: ub.Server = ub.Server(mapping=GLOBAL_CONFIG.SERVER)  # make a copy, so changes in server are not propagated
    https: bool = False
    host_ip: str = GLOBAL_CONFIG.LOCAL_IP  # changing the host_ip does not change the global config!


__all__ = [
    "GLOBAL_CONFIG",
    "ConnectionConfig"
]
