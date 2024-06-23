from queue import Queue
import signal
import time

from CommunicationMiddleware.middleware import Communicator
from Persistance.MetadataHandler import MetadataHandler
from Persistance.log import AckedBatch, FinishedSendingResults, FinishedWriting, LogReadWriter, LogType
from utils.Batch import Batch, SeqNumGenerator
from utils.SenderID import SenderID
from utils.auxiliar_functions import send_all

GATEWAY_QUEUE_NAME = 'Gateway'
GATEWAY_SENDER_ID = SenderID(0,0,1)
PERSISTANCE_DIR = '/persistance_files/'
LOG_FILENAME = 'log_out.bin'

class GatewayOut():
    def __init__(self, gateway_conn, eof_to_receive):
        self.com = None
        self.sigterm_queue = Queue()
        self.eof_to_receive = eof_to_receive
        self.gateway_conn = gateway_conn
        self.finished = False
        
        self.metadata_handler = None
        self.logger = None
        
        self.clients_sockets = {}
        self.pending_eof = {}
        self.last_received_batch = {}
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOut] SIGTERM detected\n\n")
        self.sigterm_queue.put(True)
        self.finished = True
        self.close()

    def connect(self):
        self.com = Communicator.new(self.sigterm_queue, auto_ack=False)
        if not self.com:
            return False
        return True

    def close(self):
        for socket, _pending_eof in self.clients_sockets.values():
            socket.close()
        self.com.close_connection()

    def start(self):
        if not self.load_from_disk(): 
            return None
        if not self.connect():
            return None
        if not self.initialize_based_on_last_execution():
            print("Error initializing from log")
            return None
        try:
            self.loop()
        except OSError as e :
            print("[GatewayOut] Socket disconnected: {e}")
        self.close()

    def is_dup_batch(self, batch):
        sender_last_seq_num = self.last_received_batch.get(batch.sender_id, None)
        if sender_last_seq_num != None and sender_last_seq_num == batch.seq_num:
            print(f"[Worker {self.id}] Skipping dupped batch {batch.seq_num}, from sender {batch.sender_id}")
            return True
        return False

    def loop(self):
        while not self.finished:
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if batch_bytes == None:
                print(f"[GatewayOut] Disconnected from MOM")
                break
            batch = Batch.from_bytes(batch_bytes)

            self.get_clients_until(batch.client_id)

            if not batch or self.is_dup_batch(batch):
                if not self.com.acknowledge_last_message():
                    print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                    break
                continue
            
            if not self.proccess_batch(batch):
                break

            self.dump_to_disk(batch)

            if not self.acknowledge_last_message():
                break

            if self.pending_eof.get(batch.client_id, None) == 0:
                if not self.finished_client(batch.client_id):
                    break
            
            self.logger.clean()

    def add_client(self, client_id, client_socket):
        print(f"[GatewatOut] Received new client with id: {id}")
        if id not in self.clients_sockets:
            self.pending_eof[id] = self.eof_to_receive
            self.metadata_handler.dump_eof_to_receive(id, self.eof_to_receive)
        self.clients_sockets[id] = client_socket

    def get_clients_until(self, client_id):
        while not self.finished:
            if not self.gateway_conn.poll():
                if client_id in self.clients_sockets:
                    break
                else:
                    time.sleep(0.1)
                    continue
            id, client_socket = self.gateway_conn.recv()

            if client_socket == None:
                self.clients_sockets[client_id] = None
            else:
                self.add_client(id, client_socket)
    
    def send_batch_to_client(self, client_id, batch):
        if self.clients_sockets[client_id] == None:
            return True
        try:
            send_all(self.clients_sockets[client_id], batch.to_bytes())
        except OSError as e:
            print(f"Disconected from client {client_id}, {e}")
            if self.finished:
                return False
            self.clients_sockets[client_id] = None
            return False

    def proccess_batch(self, batch):
        client_id = batch.client_id
        if not self.clients_sockets.get(client_id, None):
            if not self.com.nack_last_message():
                print("[GatewayOut] Disconected from communicator while nacking message")
                return False
            return True
            
        if batch.is_empty():
            self.pending_eof[client_id] -= 1
            print(f"[GatewayOut] Pending EOF to receive: {self.pending_eof[client_id]}")
        else:
            batch_to_send = batch.copy_keeping_fields(GATEWAY_SENDER_ID)
            print(f"[GatewayOut] Sending result to client {client_id} with {batch.size()} elements")
            self.send_batch_to_client(client_id, batch_to_send)
        return True
    
    def dump_to_disk(self, received_batch):
        self.metadata_handler.dump_metadata_to_disk(self.last_received_batch, self.pending_eof, received_batch)
        self.last_received_batch[received_batch.sender_id] = received_batch.seq_num
        self.logger.log(FinishedWriting())

    def acknowledge_last_message(self):
        if not self.communicator.acknowledge_last_message():
            print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
            return False
        self.logger.log(AckedBatch())
        return True

    def remove_client(self, client_id):
        self.clients_sockets.pop(client_id)
        self.pending_eof.pop(client_id)
        self.metadata_handler.remove_client(client_id)

    def finished_client(self, client_id):
        print(f"[Gateway] No more EOF to receive. Sending EOF to client {client_id}")
        if not self.send_batch_to_client(client_id, Batch.eof(client_id, GATEWAY_SENDER_ID)):
            return False
        self.logger.log(FinishedSendingResults(client_id, SeqNumGenerator.seq_num))
        self.remove_client(client_id)
        return True

    def set_previouse_metadata(self):
        last_sent_seq_num, self.pending_eof, self.last_received_batch = self.metadata_handler.load_stored_metadata()
        SeqNumGenerator.set_seq_num(last_sent_seq_num)

    def load_metadata(self):
        self.metadata_handler = MetadataHandler.new(PERSISTANCE_DIR, self.logger)
        if not self.metadata_handler:
            print(f"[Worker [{self.id}]] Error Opening Metadata")
            return False
        self.set_previouse_metadata()
        return True

    def load_from_disk(self):
        self.logger = LogReadWriter.new(PERSISTANCE_DIR + LOG_FILENAME)
        if not self.logger:
            print(f"Failed to open log")
            return False
        
        if not self.load_metadata():
            return False
        return True
    
    def intialize_based_on_log_changing_file(self, log):
        for key, old_entry in zip(log.keys, log.old_entries):
            if old_entry == None:
                self.metadata_handler.storage.remove(key)
            else:
                self.metadata_handler.storage.store(key, old_entry)

        self.set_previouse_metadata()
        self.send_last_execution_clients()
        return True

    def handle_any_finished_client(self):
        finished_client = None
        for client_id, pending_eof in self.pending_eof.items():
            if pending_eof == 0:
                finished_client = client_id

        if finished_client == None:
            return True

        self.get_clients_until(finished_client)
        if not self.finished_client(finished_client):
            return False

        return True

    def any_more_messages(self):
        return self.com.pending_messages(GATEWAY_QUEUE_NAME) > 0

    def initialize_based_on_log_finished_writing(self, log):
        #recibir un batch
        batch = None
        if self.any_more_messages():
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if not batch_bytes:
                print(f"[Worker {self.id}] Disconnected from MOM, while receiving_message")
                return False
            batch = Batch.from_bytes(batch_bytes)
        
            if not batch or self.is_dup_batch(batch):
                if not self.com.acknowledge_last_message():
                    print(f"[Worker {self.id}] Disconnected from MOM, while acking_message")
                    return False
            else:
                if not self.com.nack_last_message():
                    print(f"[Worker {self.id}] Disconnected from MOM, while nacking_message")
                    return False
        
        self.logger.log(AckedBatch())
        return self.handle_any_finished_client()

    def initialize_based_on_log_acked_batch(self, log):
        return self.handle_any_finished_client()
    
    def initialize_based_on_log_finished_sending_results(self, log):
        SeqNumGenerator.set_seq_num(log.batch_seq_num)
        self.metadata_handler.update_seq_num()
        self.remove_client(log.client_id)
        return True
    
    def send_last_execution_clients(self):
        for client_id in self.pending_eof.keys():
            self.gateway_conn.send(client_id)
        self.gateway_conn.send(None)

    def initialize_based_on_last_execution(self):
        last_log = self.logger.read_last_log()
        print(last_log)
        if not last_log:
            self.send_last_execution_clients()
            return True
        
        if last_log.log_type != LogType.ChangingFile:
            self.send_last_execution_clients()

        switch = {
            LogType.ChangingFile: self.intialize_based_on_log_changing_file,
            LogType.FinishedWriting: self.initialize_based_on_log_finished_writing,
            LogType.AckedBatch: self.initialize_based_on_log_acked_batch,
            LogType.FinishedSendingResults: self.initialize_based_on_log_finished_sending_results
        }

        with open(PERSISTANCE_DIR + 'log_type' + self.id.__repr__() + '.txt', "a") as file:
            file.write(f'{int(last_log.log_type)}\n')

        if switch[last_log.log_type](last_log):
            self.logger.clean()
            return True
        return False

def gateway_out_main(recv_conn, eof_to_receive):
    gateway_out = GatewayOut(recv_conn, eof_to_receive)
    gateway_out.start()
