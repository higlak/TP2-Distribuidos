import pika
import pika.exceptions
import time

STARTING_RABBIT_WAIT = 1
MAX_ATTEMPTS = 6

class ConsumerQueues():
    def __init__(self):
        self.queues = {}

    def add_queue(self, exchange_name, generator):
        self.queues[exchange_name] = generator

    def recv_from(self, exchange_name):
        try:
            generator = self.queues[exchange_name]
            method_frame, _header_frame, body = next(generator)
            
            return body, method_frame
        except (pika.exceptions.ChannelClosed, 
                pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ChannelClosedByClient):
            print ("se cerro")
            return None
    
    def contains(self, exchange_name):
        return exchange_name in self.queues.keys()

class Communicator():
    def __init__(self, prefetch_count=0):
        self.consumer_queues = ConsumerQueues()
        self.producer_exchange_names = set()
        i = STARTING_RABBIT_WAIT
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except:
                if i > 2**MAX_ATTEMPTS:
                    print("[Client] Could not connect to RabbitMQ. Max attempts reached")
                    return None
                print(f"Rabbit not ready, sleeping {i}s")
                time.sleep(i)
                i *= 2
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
        generator = self.channel.consume(queue=exchange_name)
        self.consumer_queues.add_queue(exchange_name, generator)
        self.channel.queue_bind(exchange=exchange_name, queue=exchange_name)

    def produce_message(self, exchange_name, message):
        if not exchange_name in self.producer_exchange_names:
            print("Setting prod exchange")
            self.set_producer_exchange(exchange_name)
        self.channel.basic_publish(exchange=exchange_name, body=message, routing_key='')

    def produce_message_n_times(self, exchange_name, message, n):
        for i in range(n):
            print("Sending nth time: ",i)
            self.produce_message(exchange_name, message)

    def consume_message(self, exchange_name):
        if not self.consumer_queues.contains(exchange_name):
            print("Setting cons exchange")
            self.set_consumer_queue(exchange_name)
        message, method = self.consumer_queues.recv_from(exchange_name)
        if message == None:
            return bytearray([])
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        return bytearray(message)

    def close_connection(self):
        self.channel.close()