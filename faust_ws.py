import ast
import asyncio
import os

import websockets
from websockets.legacy.server import WebSocketServerProtocol

from faust_app import CONNECTED_CLIENTS, app

# Get config from env
WS_HOST = os.getenv('WS_HOST', 'localhost')
WS_PORT = os.getenv('WS_PORT', '8765')


def remove_closed_sockets():
    # Create a 'keys' list to avoid
    # RuntimeError: dictionary changed size during iteration
    keys = list(CONNECTED_CLIENTS.keys())
    for key in keys:
        if CONNECTED_CLIENTS[key].closed:
            CONNECTED_CLIENTS.pop(key)


def open_socket(client_id, ws):
    if client_id not in CONNECTED_CLIENTS:
        CONNECTED_CLIENTS[client_id] = ws


async def on_message(ws: WebSocketServerProtocol) -> None:
    remove_closed_sockets()
    async for message in ws:
        message = ast.literal_eval(message)
        msg_id = message['id']
        msg_headers = ast.literal_eval(message['headers'])
        msg_body = message['body']

        open_socket(msg_id, ws)
        await app.send('shipper',
                       value=msg_body,
                       key=msg_id,
                       headers=msg_headers)


async def socket():
    async with websockets.serve(on_message, WS_HOST, WS_PORT):
        await asyncio.Future()  # run forever


app.add_future(socket())
app.main()
