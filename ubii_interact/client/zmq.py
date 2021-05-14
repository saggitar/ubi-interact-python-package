"""
Blub
"""
import zmq

class ZMQClient(object):

    def __init__(self) -> None:
        super().__init__()
        self.context = zmq.Context()

    async def connect(self):
        print("Connecting to hello world server…")
        socket = self.context.socket(zmq.SUB)
        socket.connect("tcp://localhost:8101")

