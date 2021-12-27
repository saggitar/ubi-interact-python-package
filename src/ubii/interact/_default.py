import enum
from functools import partial

import aiohttp

import ubii.proto as ub
from . import connections, protocol, constants, services
from .constants import GLOBAL_CONFIG


class UbiiStates(enum.IntEnum):
    STARTING = enum.auto()
    CONNECTED = enum.auto()
    STOPPED = enum.auto()


class StandardProtocol(protocol.UbiiProtocol[UbiiStates]):
    starting_state = UbiiStates.STARTING
    end_state = UbiiStates.STOPPED

    def __init__(self, config: constants.UbiiConfig = GLOBAL_CONFIG):
        super().__init__()
        self.aiohttp_session: aiohttp.ClientSession
        self.config = config

    @property
    def server(self):
        return self.config.SERVER

    @property
    def constants(self):
        return self.config.CONSTANTS

    def _update_config(self, service: services.ServiceCall):
        response = ...
        server: ub.Server = response.server
        ub.Server.copy_from(self.server, server)
        ub.Constants.copy_from(self.constants, ub.Constants.from_json(server.constants_json))

    async def _on_start(self, context):
        self.aiohttp_session = aiohttp.ClientSession()
        temp_service = connections.AIOHttpRestConnection(self.config.DEFAULT_SERVICE_URL)
        temp_service.session = self.aiohttp_session

        service_map = services.ServiceMap(
            service_call_factory=partial(services.ServiceCall, transport=temp_service)
        )

        self._update_config(await service_map[self.config.CONSTANTS.DEFAULT_TOPICS.SERVICES.SERVER_CONFIG]())

        response = await service_map[self.config.CONSTANTS.DEFAULT_TOPICS.SERVICES.SERVICE_LIST]()

        client = ...

    async def _on_connect(self, context):
        pass

    state_changes = {
        (None, starting_state): _on_start,
        (starting_state, UbiiStates.CONNECTED): _on_connect
    }


async def connect(url, config: constants.UbiiConfig = constants.GLOBAL_CONFIG):
    protocol = StandardProtocol(url)
