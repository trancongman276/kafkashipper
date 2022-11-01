# KafkaShipper
Microservice to make it easier to delivery packages in Kafka.

## Idea
This service will create a Kafka application that deliver messages using Faust and communicate with Front-end using WebSocket. </br>

```
            WebSocket           Kafka-Stream
Front-end <-----------> Faust <--------------> Kafka-Mcs
```

Front-end is expected to send data with the bellow format to Faust server using Websocket:

```json
{
  "id": "32764296-e578-4035-b15e-d4e847f3e48a",
  "headers": [('1', b'mcs_1'), ('2', b'mcs_2')],
  "body": {"data": "hello world"}
}
```

Where:
- *headers* is a list(tuple(str(int), byte)). Faust will parse this to a dictionary where each tuple is an item with
the first element is the key and the second is its value. KafkaShipper will pop the lowest key and use the decoded byte
value as the next Kafka topic where the target microservice consumes from.</br>
- *body* contain the actual data will be sent to the designated microservice.</br>
- *id* is a unique identity string of front-end client.</br>

Websocket running in the background will send the given message to Faust KafkaShipper then to microservice(s).

This process will repeat until the headers is empty then Shipper will send the result back to the front-end client through Websocket.

## Usage
```commandline
python app.py worker -l INFO --without-web
```
