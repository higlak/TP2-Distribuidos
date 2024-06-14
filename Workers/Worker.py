from multiprocessing import Process
import os

from abc import ABC, abstractmethod
import signal
import socket

from CommunicationMiddleware.middleware import Communicator
from utils.HeartbeatSender import HeartbeatSender
from utils.Batch import Batch
from utils.auxiliar_functions import append_extend
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
    def __init__(self, id, next_pools, eof_to_receive):
        self.id = id
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive
        self.pending_eof = {}
        self.signal_queue = Queue()
        self.communicator = None
        self.heartbeat_sender_thread = None
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        
    @classmethod
    def get_env(cls):
        id = Worker_ID.from_env('WORKER_ID')
        next_pools = NextPools.from_env()
        if not id or not next_pools:
            return None, None, None
        try:
            eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
        except:
            print("Invalid eof_to_receive")
            return None, None, None
        
        return id, next_pools, eof_to_receive

    def connect(self):
        communicator = Communicator.new(self.signal_queue, self.next_pools.worker_ids())
        if not communicator:
            return False
        self.communicator = communicator
        return True

    def handle_SIGTERM(self, _signum, _frame):
        print(f"\n\n [Worker [{self.id}]] SIGTERM detected \n\n")
        self.signal_queue.put(True)
        if self.communicator:
            self.communicator.close_connection()

    @abstractmethod
    def process_message(self, client_id, message):
        pass

    @abstractmethod
    def get_final_results(self, client_id):
        pass
    
    def send_final_results(self, client_id):
        fr = self.get_final_results(client_id)
        if not fr:
            return True
        final_results = []
        append_extend(final_results,fr)
        i = 0
        while True:
            batch = Batch(client_id, final_results[i:i+BATCH_SIZE])
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
            result = self.process_message(batch.client_id, message)
            if result:
                append_extend(results, result)
        return Batch(batch.client_id, results)
    
    def receive_message(self):
        worker_name = self.id.get_worker_name()
        return self.communicator.consume_message(worker_name)
        
    def start(self):    
        self.loop()
        self.communicator.close_connection()
        self.heartbeat_sender_thread.join()

    def handle_waker_leader(self):
        try:            
            heartbeat_sender = HeartbeatSender(self.id)
            self.heartbeat_sender_thread = Process(target=heartbeat_sender.start)
            self.heartbeat_sender_thread.start()
  
        except Exception as e:
            print(f"[{self.id}] Socket disconnected: {e} \n")
            return

    @abstractmethod
    def remove_client_context(self, client_id):
        pass

    def remove_client(self, client_id):
        self.pending_eof.pop(client_id)
        self.remove_client_context(client_id)
        print(f"[Worker {self.id}] Client disconnected. Worker reset")

    def handle_eof(self, client_id):
        self.pending_eof[client_id] = self.pending_eof.get(client_id, self.eof_to_receive) - 1
        print(f"[Worker {self.id}] Pending EOF to receive for client {client_id}: {self.pending_eof[client_id]}")
        if not self.pending_eof[client_id]:
            print(f"[Worker {self.id}] No more eof to receive")
            if not self.send_final_results(client_id):
                print(f"[Worker {self.id}] Disconnected from MOM, while sending final results")
                return False
            if not self.send_batch(Batch.eof(client_id)):
                print(f"[Worker {self.id}] Disconnected from MOM, while sending eof")
                return False
            self.remove_client(client_id)
        return True

    def loop(self):
        while True:
            batch_bytes = self.receive_message()
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                break
            batch = Batch.from_bytes(batch_bytes)

            if batch.is_empty():
                if not self.handle_eof(batch.client_id):
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