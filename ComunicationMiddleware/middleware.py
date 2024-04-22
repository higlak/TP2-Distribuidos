import pika
import time

import pika.exceptions

QUEUE_NAME_POSITION = 0
GENERATOR_POSITION = 1

class SubscribersQueues():
    def __init__(self):
        self.subscribers = {}

    def add_subscriber(self, exchange_name, routing_key, queue_name, generator):
        self.subscribers[(exchange_name, routing_key)] = (queue_name, generator)

    def recv_from(self, exchange_name, routing_key):
        try:
            generator = self.subscribers[(exchange_name, routing_key)][GENERATOR_POSITION]
            _method_frame, _header_frame, body = next(generator)
            return body
        except (pika.exceptions.ChannelClosed, 
                pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ChannelClosedByClient):
            print ("se cerro")
            return None
    
    def get_queue_name(self, exchange_name, routing_key):
        return self.subscribers.get((exchange_name, routing_key),(None, None))[QUEUE_NAME_POSITION]

class Communicator():
    def __init__(self, prefetch_count=1):
        self.subscribers_queues = SubscribersQueues()
        self.publisher_exchange_names = set()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def set_publisher_exchange(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.publisher_exchange_names.add(exchange_name)
    
    def set_subscriber_queue(self, exchange_name, routing_key):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        result = self.channel.queue_declare(queue='', durable=True)
        queue_name = result.method.queue
        generator = self.channel.consume(queue=queue_name, auto_ack=True)
        self.subscribers_queues.add_subscriber(exchange_name, routing_key, queue_name, generator)
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
    
    def publish_message(self, exchange_name, message, routing_key=''):
        if not exchange_name in self.publisher_exchange_names:
            self.set_publisher_exchange(exchange_name)
        self.channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)

    def receive_subscribed_message(self, exchange_name, routing_key=''):
        if not self.subscribers_queues.get_queue_name(exchange_name, routing_key):
            self.set_subscriber_queue(exchange_name, routing_key)
            
        message = self.subscribers_queues.recv_from(exchange_name, routing_key)

        return message
    
    def close_connection(self):
        self.channel.close()
