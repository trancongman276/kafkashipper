import base64

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer


class MessageConsumer:
    broker = ""
    topic = ""
    redirect = ""
    group_id = ""
    logger = None

    def __init__(self, broker, topic, group_id, redirect):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self.redirect = redirect

    def activate_listener(self):
        consumer = KafkaConsumer(bootstrap_servers=self.broker,
                                 group_id='my-group')

        consumer.subscribe(self.topic)
        producer = KafkaProducer(bootstrap_servers=self.broker)
        print("consumer is listening from topic: ", self.topic)
        value = b'this is a second test'
        try:
            for message in consumer:
                if redirect is not None:
                    producer.send(self.redirect,
                                  value,
                                  key=message.key,
                                  headers=message.headers)
                    print("Sent")
                else:
                    print(message.value)
        except KeyboardInterrupt:
            print("Aborted by user...")
        finally:
            consumer.close()


broker = '124.158.12.228:9093'
group_id = 'test-consumer-1'
topic = 'shipper_out'
redirect = None

consumer = MessageConsumer(broker, topic, group_id, redirect)
consumer.activate_listener()
