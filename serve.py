import ast
import os

from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# Load .env
load_dotenv()

# Get env
# Kafka
KAFKA_HOST = os.environ.get("KAFKA_HOST", default="staging01.gradients.host:9093")
TOPIC_PRODUCER_VI = os.environ.get("TOPIC_PRODUCER_VI", default="msd_vi2en")
TOPIC_PRODUCER_EN = os.environ.get("TOPIC_PRODUCER_EN", default="msd_gen")

TOPIC_CONSUMER_VI = os.environ.get("TOPIC_CONSUMER_VI", default="msd_out")
TOPIC_CONSUMER_EN = os.environ.get("TOPIC_CONSUMER_EN", default="msd_en2vi")

LIMIT = int(os.environ.get("LIMIT", default="100"))

# Flask
FLASK_HOST = os.environ.get("FLASK_HOST", default="0.0.0.0")
FLASK_PORT = os.environ.get("FLASK_PORT", default="1243")

SHARED_DICT = {}

# Init Flask
app = Flask(__name__)
sio = SocketIO(app, cors_allowed_origins='*', async_mode='threading', )
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Init Kafka
producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_HOST', default='kafka:9092'))


@sio.on('on_disconnect')
def disconnect(msg):
    SHARED_DICT.pop(msg['id'])


@sio.on('msd_out_vi', namespace='/kafka')
def consume_vi(msg):
    print("consume_vi:\tinit")
    _id = msg['id'] + 'vi'

    if _id not in SHARED_DICT:
        SHARED_DICT[_id] = KafkaConsumer(group_id='consumer-' + msg['id'], bootstrap_servers=KAFKA_HOST)
        SHARED_DICT[_id].assign([TopicPartition(TOPIC_CONSUMER_VI, 0)])
    consumer_vi = SHARED_DICT[_id]

    for message in consumer_vi:
        key = message.key.decode('utf-8')
        if key == _id[:-2]:
            print("consume_vi:\t", message)
            emit('msd_out_vi', {'id': key, 'data': ast.literal_eval(message.value.decode('utf-8'))[-1]})


@sio.on('msd_out_en', namespace='/kafka')
def consume_en(msg):
    print("consume_en:\tinit")
    _id = msg['id'] + 'en'

    if _id not in SHARED_DICT:
        SHARED_DICT[_id] = KafkaConsumer(group_id='consumer-' + msg['id'], bootstrap_servers=KAFKA_HOST)
        SHARED_DICT[_id].assign([TopicPartition(TOPIC_CONSUMER_EN, 0)])
    consumer_en = SHARED_DICT[_id]

    for message in consumer_en:
        key = message.key.decode('utf-8')
        if key == _id[:-2]:
            print("consume_en:\t", message)
            try:
                emit('msd_out_en', {'id': key, 'data': ast.literal_eval(message.value.decode('utf-8'))[-1]})
            except Exception as e:
                print(e)


@sio.on('msd_in', namespace='/kafka')
def mcs_in(msg):
    print("produce:\t", msg)
    # msg: {'id', 'header', 'data'}
    producer.send(topic,
                  value=str([msg['id'], msg['text']]).encode(),
                  key=bytes(msg['id'], 'utf-8'))
    producer.flush()


if __name__ == '__main__':
    sio.run(app, host=FLASK_HOST, port=FLASK_PORT, allow_unsafe_werkzeug=True)
