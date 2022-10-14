# kafkashipper
Micro-service to make it easier to delivery packages in Kafka

# Idea
- This will create a Kafka application that will deliver packages using Faust and 
communicate with Front-end with WebSocket.
</br>
```
     ws           kafka-stream
FE <----> Faust <--------------> Kafka-mcs
```
- FE will send data with bellow format to Faust server using Websocket: 

`JSON({'id': ... , 'header': ..., 'data': ...})` 

- Where header is the ***list of mcs*** that project need,</br>
data is the contain (text, image, etc.)</br>
id is the identity of the FE.

- This Websocket is running in background will send the given message to 
Faust KafkaShipper topic and sent to the mcs(s). 

- Finally, this process will repeat until the list of header is empty and send back
to the FE through Websocket.

# Usages
- Run faust2 first, let it on
```shell
python faust2.py worker -l INFO --without-web
```
- Run serve.py
```shell
python serve.py worker -l INFO --without-web
```
