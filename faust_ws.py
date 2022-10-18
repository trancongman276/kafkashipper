import os
import ast
import asyncio
import websockets

import dotenv
dotenv.load_dotenv()

from jsonschema import validate
from jsonschema.exceptions import ValidationError

from faust_app import app, connected_clients
from websockets.legacy.server import WebSocketServerProtocol

# Input message format
schema = {
    "type": "object",
    "properties": {
        "header": {"type": "array"},
        "body": {"type": "array"},
        "id": {"type": "string"}
    }
}

# Get config from env
ws_host = os.getenv('WS_HOST', 'localhost')
ws_port = os.getenv('WS_PORT', '8765')


async def on_message(ws, message):
    """
    Solve for each message
    :param ws:          WebSocket Server Protocol
    :param message:     Message receive from WebSocket
    :return:
    """
    msg = ast.literal_eval(message)
    try:
        # Validate json input
        validate(msg, schema=schema)
    except ValidationError as e:
        await ws.send(str(e))
        return
    # Get ID from WS message
    _id = msg['id']
    # Init ws data if it hasn't existed
    if _id not in connected_clients:
        connected_clients[_id] = {'ws': ws}
    connected_clients[_id]['data'] = message
    # Send to kafka
    await app.send('kafka_shipper_in', value=message)


async def on_messages(ws: WebSocketServerProtocol) -> None:
    # Check for closed WS
    for i in range(len(connected_clients)):
        key = list(connected_clients.keys())[i]
        if connected_clients[key]['ws'].closed:
            connected_clients.pop(key)
    # Solve data
    async for message in ws:
        await on_message(ws, message)


async def socket():
    async with websockets.serve(on_messages, ws_host, ws_port):
        await asyncio.Future()  # run forever


app.add_future(socket())
app.main()
