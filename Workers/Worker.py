import os

from abc import ABC, abstractmethod

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch

ID_SEPARATOR = '.'
GATEWAY_EXCHANGE_NAME = 'GATEWAY_EXCHANGE'

class Worker_ID():
    def __init__(self, query, pool_id, worker_num):
        self.query = query
        self.pool_id = pool_id
        self.worker_num = worker_num
    
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
    
    def next_exchange_name(self):
        return f'{self.query}.{int(self.pool_id)+1}'
    
    def __repr__(self):
        return f'{self.query}.{self.pool_id}.{self.worker_num}'
    
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
        
        self.communicator = Communicator()
        if not self.communicator:
            return None

    @abstractmethod
    def process_message(self, message):
        pass

    def process_batch(self, batch):
        results = []
        for message in batch:
            result = self.process_message(message)
            if result:
                append_extend(results, result)
        return Batch(results)
    
    def receive_message(self):
        exchange_name = self.id.get_exchange_name()
        return self.communicator.consume_message(exchange_name)
        
    def start(self):
        self.loop()
        self.communicator.close_connection()

    def loop(self):
        while True:
            print(f"[Worker {self.id}] Waiting for message...")
            batch_bytes = self.receive_message()
            if batch_bytes == None:
                print(f"[Worker {self.id}] Error while consuming")
                break
            batch = Batch.from_bytes(batch_bytes)
            print(f"[Worker {self.id}] Received batch with {batch.size()} elements")
            
            if batch.is_empty():
                print(f"[Worker {self.id}] Received EOf")
                self.send_batch(batch)
                return    
            
            result_batch = self.process_batch(batch)
            print(f"[Worker {self.id}] Message proccesed")
            if not result_batch.is_empty():
                self.send_batch(result_batch)
                print(f"[Worker {self.id}] Message sending batch with {result_batch.size()}", )

    def send_batch(self, batch):
        if self.next_pool_workers == 0:
            self.communicator.produce_message(GATEWAY_EXCHANGE_NAME, batch.to_bytes())
        else:
            exchange_name = self.id.next_exchange_name()
            if batch.is_empty():
                self.propagate_eof(exchange_name)
            else:
                self.communicator.produce_message(exchange_name, batch.to_bytes())

    def propagate_eof(self, exchange_name):
        print("proximos workers: ", self.next_pool_workers)
        self.communicator.produce_message_n_times(exchange_name, Batch([]).to_bytes(), self.next_pool_workers)

def append_extend(l, element_or_list):
    if isinstance(element_or_list, list):
        l.extend(element_or_list)
    else:
        l.append(element_or_list)

