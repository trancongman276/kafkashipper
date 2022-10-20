import logging
import os

from faust import App

# Get config from env
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka://staging01.gradients.host:9093")

# Init Kafa
app = App('shipper', broker=[KAFKA_HOST], value_serializer='raw', debug=True)
logger = logging.getLogger('kafka_shipper')
CONNECTED_CLIENTS = {}

shipper = app.topic('shipper')


@app.agent(shipper)
async def consume_pipe_in(stream):
    async for event in stream.events():
        e_headers = event.headers
        e_id = event.key
        e_value = event.value

        if not len(e_headers):
            CONNECTED_CLIENTS[e_id].send(e_value)
            continue

        next_topic = e_headers.pop(min(e_headers.keys())).decode()
        topic = app.topic(next_topic)
        await topic.send(value=e_value, headers=e_headers, key=e_id)
