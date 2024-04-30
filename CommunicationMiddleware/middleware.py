import pika
import pika.exceptions
import time

STARTING_RABBIT_WAIT = 1
MAX_ATTEMPTS = 6

class ConsumerQueues():
    def __init__(self):
        self.queues = {}

    def add_queue(self, queue_name, generator):
        self.queues[queue_name] = generator

    def recv_from(self, queue_name):
        try:
            generator = self.queues[queue_name]
            method_frame, _header_frame, body = next(generator)
            
            return body, method_frame
        except (pika.exceptions.ChannelClosed, 
                pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ChannelClosedByClient):
            print ("se cerro")
            return None
    
    def contains(self, queue_name):
        return queue_name in self.queues.keys()

class ProducerGroup():
    def __init__(self, producer_queues):
        self.producer_queues = producer_queues
        self.i = 0
    
    def next(self):
        queue = self.producer_queues[self.i]
        self.i = (self.i + 1) % len(self.producer_queues)
        return queue
    
    def __iter__(self):
        return iter(self.producer_queues)

    def __next__(self):
        return next(self.producer_queues)
    
    def __repr__(self):
        return f"{self.producer_queues}"

class Communicator():
    def __init__(self, producer_groups={}, prefetch_count=1):
        self.consumer_queues = ConsumerQueues() 
        self.producer_groups = {group:ProducerGroup(members) for group, members in producer_groups.items() }
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
        self.set_producer_queues()

    def next_producer_queue(self, group):
        return self.producer_groups[group].next()
    
    def set_producer_queues(self):
        for members in self.producer_groups.values():
            for queue_name in members:
                self.channel.queue_declare(queue=queue_name)

    def set_consumer_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name)
        generator = self.channel.consume(queue=queue_name)
        self.consumer_queues.add_queue(queue_name, generator)

    def produce_message(self, message, group):
        queue_name = self.next_producer_queue(group)
        self.channel.basic_publish(exchange="", body=message, routing_key=queue_name)

    def produce_message_n_times(self, message, group, n):
        for i in range(n):
            self.produce_message(message, group )

    def produce_to_all_group_members(self, message):
        for group, members in self.producer_groups.items():
            for _member in members:
                self.produce_message(message, group) 
    
    def produce_to_all_groups(self, message):
        for group in self.producer_groups.keys():
            self.produce_message(message, group)

    def consume_message(self, queue_name):
        if not self.consumer_queues.contains(queue_name):
            print("Setting cons queue")
            self.set_consumer_queue(queue_name)
        message, method = self.consumer_queues.recv_from(queue_name)
        if message == None:
            return bytearray([])
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        return bytearray(message)

    def contains_producer_group(self, group):
        return group in self.producer_groups.keys()

    def close_connection(self):
        self.channel.close()