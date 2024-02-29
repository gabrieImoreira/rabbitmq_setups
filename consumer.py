import pika

class RabbitmqConsumer:
    def __init__(self, callback) -> None:
        self.__host = 'localhost'
        self.__port = 5672
        self.__username = 'guest'
        self.__password = 'guest'
        self.__queue = 'data_queue'
        self.__callback = callback
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
        channel.queue_declare(queue=self.__queue, durable=True)
        channel.basic_consume(queue=self.__queue, on_message_callback=self.__callback, auto_ack=True)

        return channel

    def start_consuming(self):
        print(f'Listen RabbitMQ on port 5672')
        self.__channel.start_consuming()
    
    def stop_consuming(self):
        self.__channel.stop_consuming()
    
def my_callback(ch, method, properties, body):
    print(f" [x] Received {body}")

rabbitmq_consumer = RabbitmqConsumer(my_callback)
rabbitmq_consumer.start_consuming()