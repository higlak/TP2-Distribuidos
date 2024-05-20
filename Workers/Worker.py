import os

from abc import ABC, abstractmethod
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.auxiliar_functions import append_extend, InstanceError
from utils.QueryMessage import query_to_query_result
from utils.NextPools import NextPools, GATEWAY_QUEUE_NAME 
from queue import Queue

ID_SEPARATOR = '.'
BATCH_SIZE = 25

class Worker_ID():
    def __init__(self, query, pool_id, worker_num):
        self.query = query
        self.pool_id = pool_id
        self.worker_num = worker_num
    
    @classmethod
    def from_env(cls, env_var):
        env_id = os.getenv(env_var)
        if not env_id:
            print("Invalid worker id")
            return None
        query, pool_id, id = env_id.split(ID_SEPARATOR)
        return Worker_ID(query, pool_id, id)

    def get_worker_name(self):
        return f'{self.query}.{self.pool_id}.{self.worker_num}'
    
    def __repr__(self):
        return f'{self.query}.{self.pool_id}.{self.worker_num}'
    
class Worker(ABC):
    def __init__(self):
        self.id = Worker_ID.from_env('WORKER_ID')
        self.next_pools = NextPools.from_env()
        if not self.id or not self.next_pools:
            raise InstanceError
        
        try:
            self.eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
        except:
            print("Invalid eof_to_receive")
            raise InstanceError

        self.communicator = None
        self.signal_queue = Queue()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        
        self.communicator = Communicator.new(self.signal_queue, self.next_pools.worker_ids())

    def handle_SIGTERM(self, _signum, _frame):
        print(f"\n\n [Worker [{self.id}]] SIGTERM detected \n\n")
        self.signal_queue.put(True)
        if self.communicator:
            self.communicator.close_connection()

    @abstractmethod
    def process_message(self, message):
        pass

    @abstractmethod
    def get_final_results(self, message):
        pass
    
    def send_final_results(self):
        fr = self.get_final_results()
        if not fr:
            return True
        final_results = []
        append_extend(final_results,fr)
        i = 0
        while True:
            batch = Batch(final_results[i:i+BATCH_SIZE])
            if batch.size() == 0:
                break
            if not self.send_batch(batch):
                return False
            i += BATCH_SIZE
        print(f"[Worker {self.id}] Sent {i} final results")
        return True

    def transform_to_result(self, message):
        if self.communicator.contains_producer_group(GATEWAY_QUEUE_NAME):
            message.msg_type = query_to_query_result(self.id.query)
        return message

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

    @abstractmethod
    def reset_context(self):
        pass

    def reset(self):
        try:
            self.eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
        except:
            print("Invalid eof_to_receive")
            raise InstanceError
        self.reset_context()
        print(f"[Worker {self.id}] Client disconnected. Worker reset")

    def handle_eof(self):
        self.eof_to_receive -= 1
        print(f"[Worker {self.id}] Pending EOF to receive: {self.eof_to_receive}")
        if not self.eof_to_receive:
            print(f"[Worker {self.id}] No more eof to receive")
            if not self.send_final_results():
                print(f"[Worker {self.id}] Disconnected from MOM, while sending final results")
                return False
            if not self.send_batch(Batch([])):
                print(f"[Worker {self.id}] Disconnected from MOM, while sending eof")
                return False
            self.reset()
        return True

    def loop(self):
        while True:
            batch_bytes = self.receive_message()
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                break
            batch = Batch.from_bytes(batch_bytes)
            if batch.is_empty():
                if not self.handle_eof():
                    break
            else:
                result_batch = self.process_batch(batch)
                if not result_batch.is_empty():
                    if not self.send_batch(result_batch):
                        print(f"[Worker {self.id}] Disconnected from MOM, while sending_message")
                        break

    def send_batch(self, batch: Batch):
        if batch.is_empty():
            return self.communicator.produce_to_all_group_members(batch.to_bytes())
        else:
            for pool, _next_pool_workers, shard_attribute in self.next_pools:
                if not self.communicator.produce_batch_of_messages(batch, pool, shard_attribute):
                    return False
        return True