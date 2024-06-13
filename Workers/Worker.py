import os

from abc import ABC, abstractmethod
import signal

from CommunicationMiddleware.middleware import Communicator
from Persistance.log import *
from Persistance.KeyValueStorage import KeyValueStorage
from utils.SenderID import SenderID
from utils.Batch import Batch, SeqNumGenerator
from utils.auxiliar_functions import append_extend
from utils.QueryMessage import query_to_query_result
from utils.NextPools import NextPools, GATEWAY_QUEUE_NAME 
from queue import Queue

LOG_PATH = './log.bin'
PERSISTANCE_PATH = '/persistance_files/'
METADATA_FILE_NAME = 'metadata.bin'
CLIENT_CONTEXT_FILE_NAME = 'client_context'

METADATA_KEY_BYTES = 25 + 18
METADATA_NUM_BYTES = 4
CLIENT_CONTEXT_KEY_BYTES = 512
LAST_SENT_SEQ_NUM = "last sent seq_num"
CLIENT_PENDING_EOF = "pending eof client"
LAST_RECEIVED_FROM_WORKER = "last received from worker"

BATCH_SIZE = 1024

class Worker(ABC):
    def __init__(self, id, next_pools, eof_to_receive):
        self.id = id
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive
        self.pending_eof = {}
        self.signal_queue = Queue()
        self.last_received_batch = {}
        self.client_context_storage_updates = {} # {client{key: (old_value, new value)}}

        self.communicator = None
        self.metadata_storage = None
        self.client_contexts_storage = {}
        self.logger = None
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        
    @classmethod
    def get_env(cls):
        id = SenderID.from_env('WORKER_ID')
        next_pools = NextPools.from_env()
        if not id or not next_pools:
            return None, None, None
        try:
            eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
        except:
            print("Invalid eof_to_receive")
            return None, None, None
        
        return id, next_pools, eof_to_receive
    
    @abstractmethod
    def load_context(self, path):
        pass
    
    def load_all_context(self):
        try:
            for filename in os.listdir(self.worker_folder()):
                if filename == METADATA_FILE_NAME:
                    continue
                path = os.path.join(self.worker_folder(), filename)
                if os.path.isfile(path):
                    client_id = int(filename.strip('.bin').strip(CLIENT_CONTEXT_FILE_NAME))
                    if not self.load_context(path, client_id):
                        print(f"[Worker [{self.id}]]: Could not load context: {path}")
                        return False
        except OSError as e:
            print(f"[Worker [{self.id}]]: Could not load context: {e}")
        return True

    def worker_folder(self):
        return PERSISTANCE_PATH + self.id.__repr__() + '/' 

    def load_metadata(self):
        self.metadata_storage, previouse_metadata = KeyValueStorage.new(
            self.worker_folder() + METADATA_FILE_NAME, str, METADATA_KEY_BYTES, [int], [METADATA_NUM_BYTES])
        if not self.metadata_storage or previouse_metadata == None:
            print(f"[Worker [{self.id}]] Error Opening Metadata: storage {self.metadata_storage} previouse {previouse_metadata}")
            return False

        SeqNumGenerator.set_seq_num(previouse_metadata.pop(LAST_SENT_SEQ_NUM, None))

        while len(previouse_metadata) > 0:
            entry = previouse_metadata.popitem()
            if entry[0].startswith(CLIENT_PENDING_EOF):
                sender_id = int(entry[0].strip(CLIENT_PENDING_EOF))
                self.pending_eof[entry[0]] = entry[1]
            elif entry[0].startswith(LAST_RECEIVED_FROM_WORKER):
                sender_id = SenderID.from_string(entry[0].strip(LAST_RECEIVED_FROM_WORKER))
                if sender_id == None:
                    return False
                self.last_received_batch[sender_id] = entry[1]
        return True

    def load_from_disk(self):
        path = PERSISTANCE_PATH + self.id.__repr__() + '/'
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as e:
            print(f"[Worker [{self.id}]] Error creating worker dir")
            
        if not self.load_metadata():
            return False
        if not self.load_all_context():
           return False
        self.logger = LogReadWriter.new(LOG_PATH)
        if not self.logger:
            return False
        #if not self.load_context():
        #    return False
        ##hacer quilombo de logs
        return True

    def connect(self):
        communicator = Communicator.new(self.signal_queue, self.next_pools.worker_ids(), False)
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
        #print(f"{fr}")
        if not fr:
            return True
        final_results = []
        append_extend(final_results,fr)
        i = 0
        while True:
            batch = Batch.new(client_id, self.id, final_results[i:i+BATCH_SIZE])
            if batch.size() == 0:
                break
            if not self.send_batch(batch):
                return False
            i += batch.size()
        print(f"[Worker {self.id}] Sent {i} final results")
        return True

    def transform_to_result(self, message):
        if self.communicator.contains_producer_group(GATEWAY_QUEUE_NAME):
            message.msg_type = query_to_query_result(self.id.query)
        return message

    def process_batch(self, batch):
        results = []
        if batch.is_empty():
            self.handle_eof(batch.client_id)
        else:
            for message in batch:
                result = self.process_message(batch.client_id, message)
                if result:
                    append_extend(results, result)
        return Batch.new(batch.client_id, self.id, results)
        
    
    def receive_batch(self):
        worker_name = self.id.__repr__()
        return self.communicator.consume_message(worker_name)
        
    def start(self):
        if not self.load_from_disk(): 
            return
        self.loop()
        #print(f"\n\n {self.metadata_storage.get_all_entries()}\n\n")
        self.communicator.close_connection()

    @abstractmethod
    def remove_client_context(self, client_id):
        pass

    def remove_client(self, client_id):
        self.pending_eof.pop(client_id)
        self.remove_client_context(client_id)
        print(f"[Worker {self.id}] Client disconnected. Worker reset")

    def proccess_final_results(self, client_id):
        print(f"[Worker {self.id}] No more eof to receive")
        if not self.send_final_results(client_id):
            print(f"[Worker {self.id}] Disconnected from MOM, while sending final results")
            return False
        if not self.send_batch(Batch.eof(client_id, self.id)):
            print(f"[Worker {self.id}] Disconnected from MOM, while sending eof")
            return False
        #self.remove_client(client_id)
        return True

    def handle_eof(self, client_id):
        self.pending_eof[client_id] = self.pending_eof.get(client_id, self.eof_to_receive) - 1
        print(f"[Worker {self.id}] Pending EOF to receive for client {client_id}: {self.pending_eof[client_id]}")
        #proccess_final_results

    @abstractmethod
    def get_context_storage_types(self):
        pass 

    def dump_client_updates(self, client_id, update_values): #{title: (old_value, new_value)}
        storage = self.client_contexts_storage[client_id]
        
        old_values = []
        for values in update_values:
            old_values.append(values[0])
        #self.logger.log(self.change_context_log(client_id, keys, values))

        for key, values in update_values.items():
            if values[1] == None:
                storage.remove(key)
            else:
                storage.store(key, values[1])

    def dump_all_client_contexts_to_disk(self):
        while len(self.client_context_storage_updates) > 0:
            client_id, update_values  = self.client_context_storage_updates.popitem() 

            if client_id not in self.client_contexts_storage:
                path = self.worker_folder() + CLIENT_CONTEXT_FILE_NAME + str(client_id) + '.bin'
                value_types, value_types_size = self.get_context_storage_types()
                if value_types == None or value_types_size == None:
                    continue
                self.client_contexts_storage[client_id], _ = KeyValueStorage.new(
                    path, str, CLIENT_CONTEXT_KEY_BYTES, value_types, value_types_size)
            
            self.dump_client_updates(client_id, update_values)

    def dump_metadata_to_disk(self, batch):
        if batch.is_empty():
            key = CLIENT_PENDING_EOF + str(batch.client_id)
            value = self.pending_eof[batch.client_id]
        else:
            key = LAST_RECEIVED_FROM_WORKER + str(batch.sender_id)
            value = batch.seq_num
            
        self.logger.log(ChangeMetadata([key, LAST_SENT_SEQ_NUM], [value, SeqNumGenerator.seq_num]))
        self.metadata_storage.store(LAST_SENT_SEQ_NUM, [SeqNumGenerator.seq_num])
        self.metadata_storage.store(key, [value])

    def dump_to_disk(self, batch):
        try:
            self.dump_metadata_to_disk(batch)
            self.dump_all_client_contexts_to_disk()
            self.logger.log(FinishedWriting())
        except OSError as e:
            print(f"[Worker {self.id}] Error dumping to disk: {e}")
            return False
        return True

    def send_batch(self, batch: Batch):
        if batch.is_empty():
            return self.communicator.produce_to_all_group_members(batch.to_bytes())
        else:
            for pool, _next_pool_workers, shard_attribute in self.next_pools:
                if not self.communicator.produce_batch_of_messages(batch, pool, shard_attribute):
                    return False
        self.logger.log(SentBatch())
        return True

    def is_dup_batch(self, batch):
        sender_last_seq_num = self.last_received_batch.get(batch.sender_id, None)
        if sender_last_seq_num != None and sender_last_seq_num == batch.seq_num:
            print(f"[Worker {self.id}] Skipping dupped batch {batch.seq_num}, from sender {batch.sender_id}")
            return True
        return False

    def loop(self):
        while True:
            ########### receive batch
            batch_bytes = self.receive_batch()
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                break
            batch = Batch.from_bytes(batch_bytes)
            if not batch:
                continue
            
            ########### filter dup
            if self.is_dup_batch(batch):
                if not self.communicator.acknowledge_last_message():
                    print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                    break
                continue
            self.last_received_batch[batch.sender_id] = batch.seq_num

            ############ proccess batch
            result_batch = self.process_batch(batch)

            ############ Send results
            if not result_batch.is_empty():
                if not self.send_batch(result_batch):
                    print(f"[Worker {self.id}] Disconnected from MOM, while sending_message")
                    break

            ############ bajar a disco
            if not self.dump_to_disk(batch):
                break
            self.logger.log(FinishedWriting())
            
            ############ ack batch
            if not self.communicator.acknowledge_last_message():
                print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                break
            self.logger.log(AckedBatch())

            ############ Send final results
            if self.pending_eof.get(batch.client_id, None) == 0:   #guarda con perder el client_id
                if not self.proccess_final_results(batch.client_id):
                    break
                
                ######## Remove client
                self.remove_client(batch.client_id)


if __name__ == '__main__':
    import unittest
    from unittest import TestCase
    
    from io import BytesIO
    
    class TestMetadata(TestCase):
        def test_metadata(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, 50, [int], [METADATA_NUM_BYTES])
            
            storage.store(LAST_SENT_SEQ_NUM, [2])
            storage.store(LAST_RECEIVED_FROM_WORKER + SenderID(1,1,1).__repr__(), [1])
            entires = storage.get_all_entries()
            self.assertEqual(entires.pop(LAST_SENT_SEQ_NUM), 2)
            self.assertEqual(entires.pop(LAST_RECEIVED_FROM_WORKER + SenderID(1,1,1).__repr__()), 1)
    unittest.main()