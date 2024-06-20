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

PERSISTANCE_PATH = '/persistance_files/'
LOG_FILENAME = 'log.bin'
METADATA_FILENAME = 'metadata.bin'
CLIENT_CONTEXT_FILENAME = 'client_context'
SCALE_SEPARATOR = '_S'

METADATA_KEY_BYTES = 25 + 18
METADATA_NUM_BYTES = 4
LAST_SENT_SEQ_NUM = "last sent seq_num"
CLIENT_PENDING_EOF = "pending eof client"
LAST_RECEIVED_FROM_WORKER = "last received from worker" 

BATCH_SIZE = 1024

class Worker(ABC):
    def __init__(self, id, next_pools, eof_to_receive):
        self.id = id
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive
        self.signal_queue = Queue()
        self.communicator = None

        self.pending_eof = {}
        self.last_received_batch = {}
        self.client_context_storage_updates = {} # {client{key: (old_value, new value)}}
        self.client_contexts = {} # {client {depende del accum}}

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
    def add_previous_context(self, previous_context, client_id):
        pass

    def load_context(self, path, filename,  client_id, scale_of_update_file):
        storage_types, storage_types_size = self.get_context_storage_types(scale_of_update_file)
        self.client_contexts_storage[client_id][filename], previous_context = KeyValueStorage.new(
            path, str, 2**scale_of_update_file, storage_types, storage_types_size)
        print("previouse context: ", len(previous_context))
        if not self.client_contexts_storage[client_id][filename] or previous_context == None:
            return False
        self.add_previous_context(previous_context, client_id)
        return True
    
    def load_all_context(self):
        try:
            for filename in os.listdir(self.worker_folder()):
                if filename == METADATA_FILENAME or filename == LOG_FILENAME:
                    continue
                path = os.path.join(self.worker_folder(), filename)
                if os.path.isfile(path):            
                    client_id, scale_of_file = info_from_filename(filename)
                    self.client_contexts_storage[client_id] = self.client_contexts_storage.get(client_id, {})
                    if not self.load_context(path, filename, client_id, scale_of_file):
                        print(f"[Worker [{self.id}]]: Could not load context: {path}")
                        return False
        except OSError as e:
            print(f"[Worker [{self.id}]]: Could not load context: {e}")
        return True

    def worker_folder(self):
        return PERSISTANCE_PATH + self.id.__repr__() + '/' 

    def set_previouse_metadata(self, previouse_metadata):
        SeqNumGenerator.set_seq_num(previouse_metadata.pop(LAST_SENT_SEQ_NUM, None))
        self.pending_eof = {}
        self.last_received_batch = {}

        while len(previouse_metadata) > 0:
            entry = previouse_metadata.popitem()
            if entry[0].startswith(CLIENT_PENDING_EOF):
                client_id = int(entry[0].strip(CLIENT_PENDING_EOF))
                self.pending_eof[client_id] = entry[1]
            elif entry[0].startswith(LAST_RECEIVED_FROM_WORKER):
                sender_id = SenderID.from_string(entry[0].strip(LAST_RECEIVED_FROM_WORKER))
                if sender_id == None:
                    return False
                self.last_received_batch[sender_id] = entry[1]
        return True

    def load_metadata(self):
        self.metadata_storage, previouse_metadata = KeyValueStorage.new(
            self.worker_folder() + METADATA_FILENAME, str, METADATA_KEY_BYTES, [int], [METADATA_NUM_BYTES])
        if not self.metadata_storage or previouse_metadata == None:
            print(f"[Worker [{self.id}]] Error Opening Metadata: storage {self.metadata_storage} previouse {previouse_metadata}")
            return False
        
        return self.set_previouse_metadata(previouse_metadata)

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
        self.logger = LogReadWriter.new(self.worker_folder() + LOG_FILENAME)
        if not self.logger:
            print(f"Failed to open log")
            return False
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

    def send_final_results(self, client_id, already_sent_results=0):
        fr = self.get_final_results(client_id)
        if not fr:
            return True
        
        final_results = []
        append_extend(final_results,fr)
        i = already_sent_results
        while True:
            batch = Batch.new(client_id, self.id, final_results[i:i+BATCH_SIZE])
            if batch.size() == 0:
                break
            if not self.send_batch(batch):
                return False
            i += batch.size()
            self.logger.log(SentFirstFinalResults(batch.client_id, i))
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
        result_batch = Batch.new(batch.client_id, self.id, results)
        return self.send_partial_results(result_batch)
    
    def send_partial_results(self, batch):
        if not batch.is_empty():
            if not self.send_batch(batch):
                print(f"[Worker {self.id}] Disconnected from MOM, while sending_message")
                return False
        return True
        

    def receive_batch(self):
        worker_name = self.id.__repr__()
        return self.communicator.consume_message(worker_name)
        
    def start(self):
        if not self.load_from_disk(): 
            return None
        if not self.connect():
            return None
        if not self.initialize_based_on_last_execution():
            print("Error initializing from log")
            return None
        self.loop()
        #print(f"\n\n {self.metadata_storage.get_all_entries()}\n\n")
        self.communicator.close_connection()

    @abstractmethod
    def remove_client_context(self, client_id):
        pass

    def remove_client(self, client_id):
        self.pending_eof.pop(client_id, None)
        self.remove_client_context(client_id)
        client_storage = self.client_contexts_storage.get(client_id, {})
        while len(client_storage) > 0:
            filename, storage = client_storage.popitem()
            #storage.delete()
        self.metadata_storage.remove(CLIENT_PENDING_EOF+str(client_id))
        print(f"[Worker {self.id}] Client disconnected. Worker reset")

    def proccess_final_results(self, client_id, already_sent_results=0):
        print(f"[Worker {self.id}] No more eof to receive")
        if not self.send_final_results(client_id, already_sent_results):
            print(f"[Worker {self.id}] Disconnected from MOM, while sending final results")
            return False
        if not self.send_batch(Batch.eof(client_id, self.id)):
            print(f"[Worker {self.id}] Disconnected from MOM, while sending eof")
            return False
        self.logger.log(FinishedSendingResults(client_id))
        return True

    def handle_eof(self, client_id):
        self.pending_eof[client_id] = self.pending_eof.get(client_id, self.eof_to_receive) - 1
        print(f"[Worker {self.id}] Pending EOF to receive for client {client_id}: {self.pending_eof[client_id]}")

    @abstractmethod
    def get_context_storage_types(self, scale_of_update_file):
        pass 

    def dump_client_updates(self, client_id, filename, update_values): #{title: (old_value, new_value)}
        storage = self.client_contexts_storage[client_id][filename]
        
        old_values = []
        keys = []
        for key, values in update_values.items():
            old_values.append(values[0])
            keys.append(key)
        self.logger.log(ChangingFile(filename, keys, old_values))

        for key, values in update_values.items():
            if values[1] == None:
                storage.remove(key)
            else:
                storage.store(key, values[1])

    def dump_all_client_updates_to_disk(self, client_id):
        if client_id not in self.client_contexts_storage:
            self.client_contexts_storage[client_id] = {}

        while len(self.client_context_storage_updates) > 0:
            scale_of_update_file, update_values  = self.client_context_storage_updates.popitem()
            filename = CLIENT_CONTEXT_FILENAME + str(client_id) + SCALE_SEPARATOR + str(scale_of_update_file) + '.bin'

            if filename not in self.client_contexts_storage[client_id]:
                path = self.worker_folder() + filename
                value_types, value_types_size = self.get_context_storage_types(scale_of_update_file)
                if value_types == None or value_types_size == None:
                    continue
                self.client_contexts_storage[client_id][filename], _ = KeyValueStorage.new(
                    path, str, 2**scale_of_update_file, value_types, value_types_size)
            
            self.dump_client_updates(client_id, filename, update_values)

    def dump_metadata_to_disk(self, batch):
        keys = [LAST_RECEIVED_FROM_WORKER + str(batch.sender_id), LAST_SENT_SEQ_NUM]
        old_batch_seq_num = self.last_received_batch.get(batch.sender_id, None)
        if old_batch_seq_num != None:
            old_batch_seq_num = [old_batch_seq_num]
        old_entries = [old_batch_seq_num, [SeqNumGenerator.seq_num -1]]
        new_entries = [[batch.seq_num], [SeqNumGenerator.seq_num]]
        if batch.is_empty():
            keys.append(CLIENT_PENDING_EOF + str(batch.client_id))
            old_entries.append([self.pending_eof[batch.client_id] + 1])
            new_entries.append([self.pending_eof[batch.client_id]])
        self.last_received_batch[batch.sender_id] = batch.seq_num
        self.logger.log(ChangingFile(METADATA_FILENAME, keys, old_entries))
        self.metadata_storage.store_all(keys, new_entries)

    def dump_to_disk(self, batch):
        try:
            self.dump_metadata_to_disk(batch)
            self.dump_all_client_updates_to_disk(batch.client_id)
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
        return True

    def is_dup_batch(self, batch):
        sender_last_seq_num = self.last_received_batch.get(batch.sender_id, None)
        if sender_last_seq_num != None and sender_last_seq_num == batch.seq_num:
            print(f"[Worker {self.id}] Skipping dupped batch {batch.seq_num}, from sender {batch.sender_id}")
            return True
        return False

    def acknowledge_last_message(self):
        if not self.communicator.acknowledge_last_message():
            print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
            return False
        self.logger.log(AckedBatch())
        return True

    def loop(self):
        x = 0
        while True:
            ########### receive batch
            batch_bytes = self.receive_batch()
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                break
            batch = Batch.from_bytes(batch_bytes)
            
            if self.id == SenderID(3,1,0):
                print("\n recibido: ", batch.seq_num)
                print("en metdadata: ", self.last_received_batch.get(batch.sender_id, None))
                print(self.client_contexts)

            ########### filter dup
            if not batch or self.is_dup_batch(batch):
                if not self.communicator.acknowledge_last_message():
                    print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                    break
                continue

            ############ proccess batch
            if not self.process_batch(batch):
                break

            ############ bajar a disco
            if not self.dump_to_disk(batch):
                break
            
            ############ ack batch
            if not self.acknowledge_last_message():
                break

            ############ Send final results
            if self.pending_eof.get(batch.client_id, None) == 0:   #guarda con perder el client_id
                if not self.proccess_final_results(batch.client_id):
                    break
                
                ######## Remove client
                self.remove_client(batch.client_id)
            
            ############ Clean Log
            self.logger.clean()

    def intialize_based_on_log_changing_file(self, log):
        logs = self.logger.read_while_log_type(LogType.ChangingFile)
        for log in logs:
            if log.filename == METADATA_FILENAME:
                storage = self.metadata_storage
                client_id = None
            else:
                client_id, _log_scale = info_from_filename(log.filename)
                storage = self.client_contexts_storage[client_id][log.filename]
            self.rollback(storage, client_id, log.keys, log.entries)
        return True

    def send_any_ready_final_results(self):
        finished_clients = []
        for client_id, pending_eof in self.pending_eof.items():
            if pending_eof == 0:
                finished_clients.append(client_id)
        for client_id in finished_clients:
            if not self.proccess_final_results(client_id):
                return False
            self.remove_client(client_id)
        return True

    def any_more_messages(self):
        worker_name = self.id.__repr__()
        return self.communicator.pending_messages(worker_name) > 0

    def initialize_based_on_log_finished_writing(self, log):
        #recibir un batch
        batch = None
        if self.any_more_messages():
            batch_bytes = self.receive_batch()
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                return False
            batch = Batch.from_bytes(batch_bytes)
        
        if not batch or self.is_dup_batch(batch):
            if not self.communicator.acknowledge_last_message():
                print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                return False
        else:
            if not self.communicator.nack_last_message():
                print(f"[Worker {self.id}] Disconnected from MOM, while nacking_message")
                return False
        
        self.logger.log(AckedBatch())
        return self.send_any_ready_final_results()

    def initialize_based_on_log_acked_batch(self, log):
        return self.send_any_ready_final_results()

    def initialize_based_on_log_sent_final_result(self, log):
        client_id, already_sent_logs = log.client_id, log.n
        return self.proccess_final_results(client_id, already_sent_logs)
    
    def initialize_based_on_log_finished_sending_results(self, log):
        self.remove_client(log.client_id)
        return True

    def initialize_based_on_last_execution(self):
        last_log = self.logger.read_last_log()
        print(last_log)
        switch = {
            LogType.ChangingFile: self.intialize_based_on_log_changing_file,
            LogType.FinishedWriting: self.initialize_based_on_log_finished_writing,
            LogType.AckedBatch: self.initialize_based_on_log_acked_batch,
            LogType.SentFinalResult: self.initialize_based_on_log_sent_final_result,
            LogType.FinishedSendingResults: self.initialize_based_on_log_finished_sending_results
        }
        if not last_log:
            return True
        if self.id == SenderID(3,1,0):
            with open(PERSISTANCE_PATH + 'log_type' + self.id.__repr__() + '.txt', "a") as file:
                file.write(f'{int(last_log.log_type)}\n')

        return switch[last_log.log_type](last_log)
        

    def rollback(self, storage, client_id, keys, old_entries):
        for key, old_entry in zip(keys, old_entries):
            if old_entry == None:
                storage.remove(key)
            else:
                storage.store(key, old_entry)

        if client_id == None:
            self.set_previouse_metadata(storage.get_all_entries())
        else:
            self.add_previous_context(storage.get_all_entries(), client_id)

def info_from_filename(filename):
    client_id, scale = filename.strip('.bin').strip(CLIENT_CONTEXT_FILENAME).split(SCALE_SEPARATOR)
    return int(client_id), int(scale)

if __name__ == '__main__':
    import unittest
    from unittest import TestCase
    from Workers.Accumulators import ReviewTextByTitleAccumulator
    from io import BytesIO
    import pudb; pu.db
    
    STR_LEN = 4
    INT_BYTES = 4
    TEST_CONTEXT_FILENAME = CLIENT_CONTEXT_FILENAME + str(1) + '_S' + str(STR_LEN)

    class TestMetadata(TestCase):
        def test_metadata(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, 50, [int], [METADATA_NUM_BYTES])
            
            storage.store(LAST_SENT_SEQ_NUM, [2])
            storage.store(LAST_RECEIVED_FROM_WORKER + SenderID(1,1,1).__repr__(), [1])
            entires = storage.get_all_entries()
            self.assertEqual(entires.pop(LAST_SENT_SEQ_NUM), 2)
            self.assertEqual(entires.pop(LAST_RECEIVED_FROM_WORKER + SenderID(1,1,1).__repr__()), 1)
    
    class TestInitializeBasedOnLog(TestCase):
        def get_test_worker(self,n):
            storage_file = BytesIO(b"")
            metadata_file = BytesIO(b"")
            
            storage = KeyValueStorage(storage_file, str, STR_LEN, [int], [INT_BYTES])
            storage.store("1", [1])
            storage.store("2", [2])
            storage.store("3", [3])
            storage.store("4", [4])
            
            metadata_storage = KeyValueStorage(metadata_file, str, METADATA_KEY_BYTES, [int], [INT_BYTES])
            metadata_storage.store(LAST_SENT_SEQ_NUM, [1])
            metadata_storage.store(LAST_RECEIVED_FROM_WORKER + '1.0.1', [2])
            
            worker = ReviewTextByTitleAccumulator(SenderID(1,1,1), 5, 5, None, None, None)
            worker.logger = self.get_test_logger(n)
            worker.client_contexts_storage[1] = {TEST_CONTEXT_FILENAME: storage}
            worker.metadata_storage = metadata_storage
            return worker

        def get_test_logger(self, n):
            logs = [ChangingFile(METADATA_FILENAME, [LAST_SENT_SEQ_NUM, LAST_RECEIVED_FROM_WORKER + '1.0.1'], [[0], [2]]),
                    ChangingFile(TEST_CONTEXT_FILENAME, ["2", "4"], [[1], None]),
                    FinishedWriting()]
            log_file = BytesIO(b"")
            logger = LogReadWriter(log_file)
            for i in range(n):
                logger.log(logs.pop(0))
            return logger

        def test_last_log_changing_file_only_metadata(self):
            w: Worker = self.get_test_worker(1)
            w.initialize_based_on_last_execution()
            expected_metadata_entries = {LAST_SENT_SEQ_NUM: 0, LAST_RECEIVED_FROM_WORKER + '1.0.1': 2}
            self.assertEqual(w.metadata_storage.get_all_entries(), expected_metadata_entries)
            self.assertEqual(SeqNumGenerator.seq_num, 0)
            self.assertEqual(w.pending_eof, {})
            self.assertEqual({SenderID(1,0,1):2}, w.last_received_batch)

        def test_last_log_changing_file_multiple_files(self):
            w: Worker = self.get_test_worker(2)
            w.initialize_based_on_last_execution()

            expected_metadata_entries = {LAST_SENT_SEQ_NUM: 0, LAST_RECEIVED_FROM_WORKER + '1.0.1': 2}
            expected_context = {
                "1": 1,
                "2": 1,
                "3": 3
            }

            self.assertEqual(w.metadata_storage.get_all_entries(), expected_metadata_entries)
            self.assertEqual(SeqNumGenerator.seq_num, 0)
            self.assertEqual(w.pending_eof, {})
            self.assertEqual({SenderID(1,0,1):2}, w.last_received_batch)
            self.assertEqual(w.client_contexts_storage[1][TEST_CONTEXT_FILENAME].get_all_entries(), expected_context)
            self.assertEqual(w.client_contexts[1], expected_context)

    unittest.main()