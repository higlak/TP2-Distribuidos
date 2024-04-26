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
            method_frame, header_frame, body = self.channel.consume(queue=queue_name, auto_ack=True)
            self.channel.consume(queue=queue_name, auto_ack=True)
            #_method_frame, _header_frame, body = next(generator)
            return body
        except (pika.exceptions.ChannelClosed, 
                pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ChannelClosedByClient):
            print ("se cerro")
            return None
    
    def get_queue_name(self, exchange_name, routing_key):
        return self.subscribers.get((exchange_name, routing_key),(None, None))[QUEUE_NAME_POSITION]

class Communicator():
    def __init__(self, prefetch_count=1, routing_keys=[]):
        self.subscribers_queues = SubscribersQueues()
        self.publisher_exchange_names = set()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.routing_key_iterator = RoutingKeyIterator(routing_keys)

    def set_publisher_exchange(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.publisher_exchange_names.add(exchange_name)
        time.sleep(5)
        # Obtener una lista de las colas asociadas a ese exchange
        #queue_list = self.channel.queue_declare(passive=True, arguments={'x-exchange': exchange_name})

        # Obtener el número de colas
        #num_queues = queue_list.method.message_count
        #print("\n\nAAAAAAAAAAAAAAAAAAAAAAAAA: {num_queues}\n\n")
    
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

    def publish_message_next_routing_key(self, exchange_name, message):
        routing_key = self.routing_key_iterator.next()
        self.publish_message(exchange_name, message, routing_key)

    def receive_subscribed_message(self, exchange_name, routing_key=''):
        print(f"Exchange_name: {exchange_name}, routing_key: {routing_key}")
        if not self.subscribers_queues.get_queue_name(exchange_name, routing_key):
            self.set_subscriber_queue(exchange_name, routing_key)
            
        message = self.subscribers_queues.recv_from(exchange_name, routing_key)

        return bytearray(message)
    
    def publish_to_all_routing_keys(self, exchange_name, message):
        for routing_key in self.routing_key_iterator.routing_keys:
            self.publish_message(exchange_name, message, routing_key)

    def close_connection(self):
        self.channel.close()

class RoutingKeyIterator():
    def __init__(self, list):
        if len(list) == 0:
            return None
        self.routing_keys = list
        self.actual = 0
        

    def next(self):
        routing_key = self.routing_keys[self.actual % len(self.routing_keys)]
        self.actual += 1
        return routing_key
    
        