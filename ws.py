import asyncio

import faust
import websockets
from mode import Service
from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from faust2 import pipe_in


class App(faust.App):

    def on_init(self):
        self.websockets = Websockets(self)

    async def on_start(self):
        await self.add_runtime_dependency(self.websockets)


class Websockets(Service):
    connected_clients = []

    def __init__(self, app, bind: str = 'localhost', port: int = 9999, **kwargs):
        self.app = app
        self.bind = bind
        self.port = port
        super().__init__(**kwargs)

    async def on_message(self, ws, message):
        # TODO: Produce to topic kafka
        _id = message['id']
        if _id not in self.connected_clients:
            self.connected_clients[_id] = ws
        pipe_in.send(value=message)

    async def on_messages(self,
                          ws: WebSocketServerProtocol,
                          path: str) -> None:
        try:
            async for message in ws:
                await self.on_message(ws, message)
        except ConnectionClosed:
            await self.on_close(ws)
        except asyncio.CancelledError:
            pass

    async def on_close(self, ws):
        # TODO: Do sth to stop the kafka flow?
        print(ws)
        ...

    @Service.task
    async def _background_server(self):
        await websockets.serve(self.on_messages, self.bind, self.port)
