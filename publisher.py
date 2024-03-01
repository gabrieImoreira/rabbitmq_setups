from typing import Dict
import pika
import json

class RabbitmqPublisher:
    def __init__(self) -> None:
        self.__host = 'localhost'
        self.__port = 5672
        self.__username = 'guest'
        self.__password = 'guest'
        self.__exchange = 'data_exchange'
        self.__routing_key = ''
        self.__channel = self.create_channel()
    
    def create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )    
        )
        channel = pika.BlockingConnection(connection_parameters).channel()

        return channel
    
    def send_message(self, body: Dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routing_key,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f' [x] Sent {message}')

rabbitmq_publisher = RabbitmqPublisher()
message = {'message': 'Hello World, 01/03!!!!', 'status': 'OK'}
message = [
    {"id": 1, "name": "John Doe", "email": ""},
    {"id": 2, "name": "Jane Doe", "email": ""}
]

rabbitmq_publisher.send_message(message)