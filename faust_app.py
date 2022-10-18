import os
import ast
import json
import asyncio
import logging

from faust import App

# Get config from env
ks_host = os.getenv("KAFKA_HOST", "kafka:9092")
batch_size = int(os.getenv("BATCH_SIZE", "8"))
timeout = float(os.getenv("TIMEOUT", "0.1"))

# Init Kafa
app = App('kafka_shipper', broker=[ks_host], value_serializer='raw', debug=True)
logger = logging.getLogger('kafka_shipper')
connected_clients = {}


# KafkaShipper topic
pipe_in = app.topic('kafka_shipper_in')


@app.agent(pipe_in)
async def consume_pipe_in(stream):
    async for record in stream.take(batch_size, within=timeout):
        route_ls = []
        # Take record from batch
        for value in record:
            data = ast.literal_eval(value.decode())
            # Check if message is finished or not
            if len(data['header']) == 0:
                _id = data['id']
                # Send back to client through WS
                connected_clients[_id]['ws'].send(connected_clients[_id]['data'])
                continue
            # Get next topic
            topic = app.topic(data['header'][0])
            data['header'] = data['header'][1:]
            route_ls.append([topic, json.dumps(data)])
        tasks = []
        # Delivery topic
        for topic, data in route_ls:
            task = asyncio.create_task(topic.send(value=data.encode()))
            tasks.append(task)
        await asyncio.gather(*tasks)
