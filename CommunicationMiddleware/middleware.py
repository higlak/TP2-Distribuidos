import hashlib
from time import sleep
import pika
import pika.channel, pika.connection
import pika.exceptions
from queue import Queue, Empty

import pika.spec
from utils.Batch import Batch

STARTING_RABBIT_WAIT = 1
MAX_ATTEMPTS = 6
MIDDLEWARE_EXCEPTIONS = (pika.exceptions.AMQPError,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.ChannelClosed, 
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.ChannelClosedByClient,
            pika.exceptions.StreamLostError,
            pika.exceptions.ChannelWrongStateError,
            OSError,
            AttributeError,
            StopIteration)

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
        except MIDDLEWARE_EXCEPTIONS:
            print ("se cerro")
            return None, None
    
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
    
    def __len__(self):
        return len(self.producer_queues)
    
    def __repr__(self):
        return f"{self.producer_queues}"
        
class Communicator():
        
    def __init__(self, connection, producer_groups={}, prefetch_count=1):
        self.connection = connection
        self.consumer_queues = ConsumerQueues()     
        self.producer_groups = {group:ProducerGroup(members) for group, members in producer_groups.items() }
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.set_producer_queues()
        self.channel.confirm_delivery()

    @classmethod
    def new(cls, signal_queue: Queue, producer_groups={}, prefetch_count=1):
        i = STARTING_RABBIT_WAIT
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except:
                if i > 2**MAX_ATTEMPTS:
                    print("[Communicator] Could not connect to RabbitMQ. Max attempts reached")
                    return None 
                print(f"Rabbit not ready, sleeping {i}s")
                try:
                    signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2
        return Communicator(connection, producer_groups, prefetch_count)        

    def set_producer_queues(self):
        for members in self.producer_groups.values():
            for queue_name in members:
                self.channel.queue_declare(queue=queue_name, durable=True)

    def set_consumer_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)
        generator = self.channel.consume(queue=queue_name)
        self.consumer_queues.add_queue(queue_name, generator)

    def produce_message(self, message, group, queue_pos=None):
        if self.connection.is_closed:
            return False
        group_to_send = self.producer_groups[group]
        if queue_pos == None:
            queue_name = group_to_send.next()
        else:
            queue_name = group_to_send.producer_queues[queue_pos]
        
        acked = False
        while not acked:
            try:
                self.channel.basic_publish(exchange="", body=message, routing_key=queue_name)
                acked = True 
            except pika.exceptions.UnroutableError:
                print("\n\nRabbit perdio un mensaje, reenviando\n\n")
            except MIDDLEWARE_EXCEPTIONS:
                return False 
        return True
    
    def produce_batch_of_messages(self, batch, group, shard_by=None):
        """
        Produces a batch into the group sharding it first by the field shard_by. 
        Objects inside of batch must implement get_attribute_to_hash method that returns a string
        """
        if shard_by == None:
            return self.produce_message(batch.to_bytes(), group, None)
        hashed_batchs = get_sharded_batchs(batch, shard_by, self.amount_of_producer_group(group))
        for batch_destination, batch in hashed_batchs.items():
            if not batch.is_empty():
                if not self.produce_message(batch.to_bytes(), group, batch_destination):
                    return False
        return True


    def produce_to_all_group_members(self, message):
        for group, members in self.producer_groups.items():
            for _member in members:
                if not self.produce_message(message, group):
                    return False
        return True

    def consume_message(self, queue_name):
        if self.connection.is_closed:
            return bytearray([])
        try:
            if not self.consumer_queues.contains(queue_name):
                self.set_consumer_queue(queue_name)
            message, method = self.consumer_queues.recv_from(queue_name)
            if message == None:
                return bytearray([])
            
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            return bytearray(message)
        except MIDDLEWARE_EXCEPTIONS:
            return bytearray([])


    def amount_of_producer_group(self, group):
        return len(self.producer_groups[group])

    def contains_producer_group(self, group):
        return group in self.producer_groups.keys()

    def close_connection(self):
        if not self.connection.is_closed:
            self.connection.close()

def get_sharded_batchs(batch, shard_by, amount_of_shards):
    hashed_messages = {}
    for msg in batch:
        string_to_hash = msg.get_attribute_to_hash(shard_by)
        hash_object = hashlib.sha256(string_to_hash.lower().encode())
        msg_destination = int(hash_object.hexdigest(), 16) % amount_of_shards
        hashed_messages[msg_destination] = hashed_messages.get(msg_destination, []) + [msg]
    
    return {w:Batch(batch.client_id, messages) for w, messages in hashed_messages.items()}