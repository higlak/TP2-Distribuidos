import pika
import time

QUEUE_NAME_POSITION = 0
GENERATOR_POSITION = 1

class SubscribersQueues():
    def __init__(self):
        self.subscribers = {}

    def add_subscriber(self, exchange_name, queue_name, generator):
        self.subscribers[exchange_name] = (queue_name, generator)

    def recv_from(self, exchange_name):
        _method_frame, _header_frame, body = next(self.subscribers[exchange_name][GENERATOR_POSITION])
        return body
    
    def get_queue_name(self, exchange_name):
        return self.subscribers.get(exchange_name,(None, None))[QUEUE_NAME_POSITION]

class Communicator():
    def __init__(self, prefetch_count=1):
        self.subscribers_queues = SubscribersQueues()
        self.publisher_exchange_names = set()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def set_publisher_exchange(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')
        self.publisher_exchange_names.add(exchange_name)
    
    def set_subscriber_queue(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')
        result = self.channel.queue_declare(queue='', durable=True)
        queue_name = result.method.queue
        generator = self.channel.consume(queue=queue_name, auto_ack=True)
        self.subscribers_queues.add_subscriber(exchange_name, queue_name, generator)
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name)
    
    def publish_message(self, exchange_name, message):
        if not exchange_name in self.publisher_exchange_names:
            self.set_publisher_exchange(exchange_name)
        self.channel.basic_publish(exchange=exchange_name, routing_key='', body=message)
        print(" [x] Sent %r" % message)
        time.sleep(1)

    def receive_subscribed_message(self, exchange_name):
        if not self.subscribers_queues.get_queue_name(exchange_name):
            self.set_subscriber_queue(exchange_name)
            
        message = self.subscribers_queues.recv_from(exchange_name)
        print("Received message: ", message)

        return message
    
    def close_connection(self):
        self.channel.close()
