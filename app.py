import ast
import asyncio
import logging
import os

import websockets
from faust import App
from websockets.legacy.server import WebSocketServerProtocol

# Get config from env
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka://staging01.gradients.host:9093")

# Get config from env
WS_HOST = os.getenv('WS_HOST', 'localhost')
WS_PORT = os.getenv('WS_PORT', '8765')

# Init Kafa
app = App('shipper', broker=[KAFKA_HOST], value_serializer='raw', debug=True)
logger = logging.getLogger('kafka_shipper')
CONNECTED_CLIENTS = {}

shipper_in = app.topic('shipper_in')
shipper_out = app.topic('shipper_out')


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
        for header in message['headers']:
            # ['key', 'topic']
            header[1] = header[1].encode()
        msg_headers = list(tuple(header) for header in message['headers'])
        msg_body = message['body']

        open_socket(msg_id, ws)
        await app.send('shipper_in',
                       value=f"{msg_body}".encode(),
                       key=f"{msg_id}".encode(),
                       headers=msg_headers)


async def socket():
    async with websockets.serve(on_message, WS_HOST, WS_PORT):
        await asyncio.Future()  # run forever


@app.agent(shipper_in)
async def consume_pipe_in(stream):
    async for event in stream.events():
        logger.info(f"{event}, {event.headers}")
        e_headers = event.headers
        e_id = event.key
        e_value = event.value

        if not len(e_headers):
            await shipper_out.send(value=e_value, key=e_id)
            logger.info("Sent to shipper_out")
            if e_id is not None:
                await CONNECTED_CLIENTS[e_id.decode()].send(e_value)
            continue

        next_topic = e_headers.pop(min(e_headers.keys())).decode()
        topic = app.topic(next_topic)
        await topic.send(value=e_value, headers=e_headers, key=e_id)


app.add_future(socket())
app.main()
