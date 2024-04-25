import os
import sys 
#sys.path.append("..")

from abc import ABC, abstractmethod
from time import sleep

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.Message import Message

ID_SEPARATOR = '.'
GATEWAY_EXCHANGE_NAME = 'GATEWAY_EXCHANGE'

class Worker_ID():
    def __init__(self, query, pool_id, routing_key):
        self.query = query
        self.pool_id = pool_id
        self.routing_key = routing_key
    
    @classmethod
    def from_env(cls, env_var):
        env_id = os.getenv(env_var)
        if not env_id:
            return None
        query, pool_id, id = env_id.split(ID_SEPARATOR)
        return Worker_ID(query, pool_id, id)

    def get_query(self):
        return self.query
    
    def get_exchange_name(self):
        return f'{self.query}.{self.pool_id}'
    
    def get_routing_key(self):
        return self.routing_key
    
    def next_exchange_name(self):
        return f'{self.query}.{int(self.pool_id)+1}'
    
class Worker(ABC):
    def __init__(self):
        self.id = Worker_ID.from_env('WORKER_ID')
        if not self.id:
            print("Missing env variables")
            return None
        
        try:
            self.next_pool_workers = int(os.getenv('NEXT_POOL_WORKERS'))
        except:
            print("Attempted to use non int value for NEXT_POOL_WORKERS")
            return None
        
        if self.next_pool_workers == 0:
            self.communicator = Communicator()
        else:
            routing_keys = []
            for i in range(self.next_pool_workers):
                routing_keys.append(str(i+1))
            self.communicator = Communicator(routing_keys=routing_keys)


    @abstractmethod
    def process_message(self, message):
        pass

    def process_batch(self, batch):
        results = []
        for message in batch:
            result = self.process_message(message)
            if result:
                results.append(result)
        return Batch(results)
    
    def receive_message(self):
        exchange_name = self.id.get_exchange_name()
        routing_key = self.id.get_routing_key()
        print("Exchange_name: ", exchange_name)
        print("routing_key: ", routing_key)
        return self.communicator.receive_subscribed_message(exchange_name, routing_key)
        
    def start(self):
        while True:
            print(f"[Worker {self.id}] Waiting for message...")
            batch_bytes = self.receive_message()
            
            batch = Batch.from_bytes(batch_bytes)
            print(f"[Worker {self.id}] Received batch with {batch.size()} elements")
            
            if batch.is_empty():
                self.send_message(batch.to_bytes())
                print("Received Eof")
                return    
            
            result_batch = self.process_batch(batch)
            print(f"[Worker {self.id}] Message proccesed")
            if not result_batch.is_empty():
                self.send_message(result_batch.to_bytes())
                print(f"[Worker {self.id}] Message sending batch with {result_batch.size()}", )

    def send_message(self, message):
        if self.next_pool_workers == 0:
            sleep(5) #p esto es horrible
            self.communicator.publish_message(GATEWAY_EXCHANGE_NAME, message)
        else:
            exchange_name = self.id.next_exchange_name()
            self.communicator.publish_message_next_routing_key(exchange_name, message)

