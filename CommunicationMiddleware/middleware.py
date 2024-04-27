import pika
import pika.exceptions

class ConsumerQueues():
    def __init__(self):
        self.queues = {}

    def add_queue(self, exchange_name, generator):
        self.queues[exchange_name] = generator

    def recv_from(self, exchange_name):
        try:
            generator = self.queues[exchange_name]
            _method_frame, _header_frame, body = next(generator)
            return body
        except (pika.exceptions.ChannelClosed, 
                pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ChannelClosedByClient):
            print ("se cerro")
            return None
    
    def contains(self, exchange_name):
        return exchange_name in self.queues

class Communicator():
    def __init__(self, prefetch_count=1):
        self.consumer_queues = ConsumerQueues()
        self.producer_exchange_names = set()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)
    
    def set_producer_exchange(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')
        self.producer_exchange_names.add(exchange_name)
        self.channel.queue_declare(queue=exchange_name)
        self.channel.queue_bind(exchange=exchange_name, queue=exchange_name)

    def set_consumer_queue(self, exchange_name):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')
        self.channel.queue_declare(queue=exchange_name)
        generator = self.channel.consume(queue=exchange_name, auto_ack=True)
        self.consumer_queues.add_queue(exchange_name, generator)
        self.channel.queue_bind(exchange=exchange_name, queue=exchange_name)

    def produce_message(self, exchange_name, message):
        if not exchange_name in self.producer_exchange_names:
            self.set_producer_exchange(exchange_name)
        self.channel.basic_publish(exchange=exchange_name, body=message, routing_key='')

    def produce_message_n_times(self, exchange_name, message, n):
        for _ in range(n):
            self.produce_message(exchange_name, message)

    def consume_message(self, exchange_name):
        if not self.consumer_queues.contains(exchange_name):
            self.set_consumer_queue(exchange_name)
        message = self.consumer_queues.recv_from(exchange_name)
        return bytearray(message)

    def close_connection(self):
        self.channel.close()