import pika

def my_callback(ch, method, properties, body):
    print(f" [x] Received {body}")

connection_parameters = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    credentials=pika.PlainCredentials(
        username='guest',
        password='guest'
    )    
)

channel = pika.BlockingConnection(connection_parameters).channel()
channel.queue_declare(queue='data_queue', durable=True)

channel.basic_consume(queue='data_queue', on_message_callback=my_callback, auto_ack=True)

print(f'Listen RabbitMQ on port 5672')
channel.start_consuming()