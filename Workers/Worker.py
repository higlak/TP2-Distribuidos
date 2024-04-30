import os

from abc import ABC, abstractmethod

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.auxiliar_functions import get_env_list 

ID_SEPARATOR = '.'
GATEWAY_QUEUE_NAME = "Gateway"

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
    
    def get_worker_name(self):
        return f'{self.query}.{self.pool_id}.{self.worker_num}'
    
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
            next_pool_workers = get_env_list('NEXT_POOL_WORKERS')
            forward_to = get_env_list("FORWARD_TO")
            self.eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
            next_pool_queues = []
            for i in range(len(next_pool_workers)):
                if forward_to[i] == GATEWAY_QUEUE_NAME:
                    l = [GATEWAY_QUEUE_NAME]
                else:
                    l = [f'{forward_to[i]}.{j}' for j in range(int(next_pool_workers[i]))]
                next_pool_queues.append(l)
        except Exception as r:
            print(f"[Worker {self.id}] Failed converting env_vars: {r}")
            return None
        
        print("groups : ", dict(zip(forward_to, next_pool_queues)))

        self.communicator = Communicator(dict(zip(forward_to, next_pool_queues)))
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
        worker_name = self.id.get_worker_name()
        return self.communicator.consume_message(worker_name)
        
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
                self.eof_to_receive -= 1
                print(f"[Worker {self.id}] Pending EOF to receive: {self.eof_to_receive}")
                if not self.eof_to_receive:
                    print(f"[Worker {self.id}] No more eof to receive")
                    self.send_batch(batch)
                    break
            
            result_batch = self.process_batch(batch)
            print(f"[Worker {self.id}] Message proccesed")
            if not result_batch.is_empty():
                self.send_batch(result_batch)
                print(f"[Worker {self.id}] Message sending batch with {result_batch.size()}", )

    def send_batch(self, batch):
        if batch.is_empty():
            self.communicator.produce_to_all_group_members(batch.to_bytes())
        else:
            self.communicator.produce_to_all_groups(batch.to_bytes())
            
def append_extend(l, element_or_list):
    if isinstance(element_or_list, list):
        l.extend(element_or_list)
    else:
        l.append(element_or_list)

