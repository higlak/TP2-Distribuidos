from queue import Queue
import queue
import signal
import time

from CommunicationMiddleware.middleware import Communicator
from GatewayInOut.GatewayOutReceiver import GatewayOutReceiverHandler
from Persistance.MetadataHandler import MetadataHandler
from Persistance.log import AckedBatch, FinishedSendingResults, FinishedWriting, LogReadWriter, LogType
from utils.Batch import AMOUNT_OF_CLIENT_ID_BYTES, Batch, SeqNumGenerator
from utils.SenderID import SenderID
from utils.auxiliar_functions import send_all
from threading import Thread

import utils.faulty

GATEWAY_QUEUE_NAME = 'Gateway'
GATEWAY_SENDER_ID = SenderID(0,0,1)
PERSISTANCE_DIR = '/persistance_files/'
LOG_FILENAME = 'log_out.bin'
RECV_TIMEOUT = 0.1
NO_CLIENT_ID = 2**(8*AMOUNT_OF_CLIENT_ID_BYTES) - 1
TIME_FOR_RECONNECTION = 15
UNKNOWN_CLIENT_TIMEOUT = TIME_FOR_RECONNECTION + 5 

class GatewayOut():
    def __init__(self, gateway_conn, eof_to_receive):
        self.receiver_handler = None
        self.eof_to_receive = eof_to_receive
        self.gateway_conn = gateway_conn
        self.finished = False
        self.last_client_id = -1
        
        self.metadata_handler = None
        self.logger = None
        
        self.clients_sockets = {}
        self.pending_eof = {}
        self.last_received_batch = {}
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOut] SIGTERM detected\n\n")
        self.close()

    def connect(self):
        self.receiver_handler = GatewayOutReceiverHandler()
        return self.receiver_handler.start()

    def close(self):
        self.finished = True
        for socket in self.clients_sockets.values():
            if socket != None:
                socket.close()
        if self.receiver_handler != None:
            self.receiver_handler.close()

    def start(self):
        if not self.load_from_disk():
            return None
        if not self.connect():
            return False
        if not self.initialize_based_on_last_execution():
            print("[GatewayOut] Error initializing from log")
            return None

        self.loop()
        print("[GatewayOut] Finishing")
        self.close()

    def is_dup_batch(self, batch):
        sender_last_seq_num = self.last_received_batch.get(batch.sender_id, None)
        if sender_last_seq_num != None and sender_last_seq_num == batch.seq_num:
            print(f"[GatewayOut] Skipping dupped batch {batch.seq_num}, from sender {batch.sender_id}")
            return True
        return False

    def recv_batch_and_get_clients(self):
        while not self.finished:
            self.get_clients()
            try:
                return self.receiver_handler.recv_batch(RECV_TIMEOUT)
            except queue.Empty:
                continue

    def loop(self):
        while not self.finished:
            batch = self.recv_batch_and_get_clients()
            if not batch:
                print(f"[GatewayOut] Disconnected from MOM")
                break

            if self.is_dup_batch(batch):
                if not self.receiver_handler.ack_batch():
                    print(f"[GatewayOut] Disconnected from MOM, while acking_message")
                    break
                continue

            if not self.get_clients(batch.client_id):
                break
            
            if not self.proccess_batch(batch):
                break

            self.dump_to_disk(batch)

            if not self.acknowledge_last_message():
                break

            if self.pending_eof.get(batch.client_id, None) == 0:
                print("[GatewayOut] finish")
                if not self.finished_client(batch.client_id):
                    break
            
            self.logger.clean()

    def add_client(self, client_id, client_socket):
        print(f"[GatewatOut] Received new client with id: {client_id}")
        self.las_client_id = max(client_id, self.las_client_id)
        if client_id not in self.clients_sockets:
            print("EN ADD CLIENT: en el get ", self.pending_eof.get(client_id, "NO hay nada"))
            self.pending_eof[client_id] = self.pending_eof.get(client_id, self.eof_to_receive)
            self.metadata_handler.dump_new_client(client_id, self.pending_eof[client_id])
            self.logger.clean()
        self.clients_sockets[client_id] = client_socket
        self.gateway_conn.send(client_id)

    def get_clients(self, until_client_id=None):
        finish_time = time.time() + UNKNOWN_CLIENT_TIMEOUT
        while not self.finished:
            if until_client_id != None:
                print("Waiting for ", until_client_id)
            #print("[GatewayOut] waiting for ", until_client_id)
            try:
                if not self.gateway_conn.poll():
                    if until_client_id == None or until_client_id in self.clients_sockets:
                        break
                    else:
                        if finish_time < time.time():
                            self.clients_sockets[client_id] = None
                            self.pending_eof[client_id] = self.eof_to_receive
                            break
                        time.sleep(RECV_TIMEOUT)
                        continue
                client_id, client_socket = self.gateway_conn.recv()
            except Exception as e:
                print("[GatewayOut] Disconected from gateway", e)
                return

            if client_socket == None:
                print(f"[GatewayOut] Recibi {client_id}, {client_socket}")
                if client_id == NO_CLIENT_ID: 
                    self.gateway_conn.send(client_id)
                else:
                    self.clients_sockets[client_id] = None
                    self.pending_eof[client_id] = self.eof_to_receive
            else:
                self.add_client(client_id, client_socket)
        return not self.finished
    
    def send_batch_to_client(self, client_id, batch):
        if self.clients_sockets[client_id] == None:
            return True
        try:
            send_all(self.clients_sockets[client_id], batch.to_bytes())
        except OSError as e:
            print(f"[GatewayOut] Disconected from client {client_id}, {e}")
            if self.finished:
                return False
            self.clients_sockets[client_id] = None
        return True

    def proccess_batch(self, batch):
        client_id = batch.client_id
            
        if batch.is_empty():
            self.pending_eof[client_id] -= 1
            print(f"[GatewayOut] Pending EOF to receive: {self.pending_eof[client_id]} of client: {client_id}")
        else:
            batch_to_send = batch.copy_keeping_fields(GATEWAY_SENDER_ID)
            print(f"[GatewayOut] Sending result to client {client_id} with {batch.size()} elements")
            return self.send_batch_to_client(client_id, batch_to_send)
        return True
    
    def dump_to_disk(self, received_batch):
        self.metadata_handler.dump_metadata_to_disk(self.last_received_batch, self.pending_eof, received_batch)
        self.last_received_batch[received_batch.sender_id] = received_batch.seq_num
        self.logger.log(FinishedWriting())

    def acknowledge_last_message(self):
        if not self.receiver_handler.ack_batch():
            print(f"[GatewayOut] Disconnected from MOM, while acking_message")
            return False
        self.logger.log(AckedBatch())
        return True

    def remove_client(self, client_id):
        if self.clients_sockets[client_id] != None:
            self.clients_sockets.pop(client_id)
            self.pending_eof.pop(client_id)
        self.metadata_handler.remove_client(client_id)

    def finished_client(self, client_id):
        print(f"[GatewayOut] No more EOF to receive. Sending EOF to client {client_id}")
        if not self.send_batch_to_client(client_id, Batch.eof(client_id, GATEWAY_SENDER_ID)):
            return False
        self.logger.log(FinishedSendingResults(client_id, SeqNumGenerator.seq_num))
        self.remove_client(client_id)
        return True

    def set_previouse_metadata(self):
        last_sent_seq_num, self.pending_eof, self.last_received_batch, self.las_client_id = self.metadata_handler.load_stored_metadata(True)
        SeqNumGenerator.set_seq_num(last_sent_seq_num)

    def load_metadata(self):
        self.metadata_handler = MetadataHandler.new(PERSISTANCE_DIR, self.logger)
        if not self.metadata_handler:
            print(f"[GatewayOut] Error Opening Metadata")
            return False
        self.set_previouse_metadata()
        return True

    def load_from_disk(self):
        self.logger = LogReadWriter.new(PERSISTANCE_DIR + LOG_FILENAME)
        if not self.logger:
            print(f"[GatewayOut] Failed to open log")
            return False
        
        if not self.load_metadata():
            return False
        return True
    
    def intialize_based_on_log_changing_file(self, log):
        for key, old_entry in zip(log.keys, log.entries):
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

        self.get_clients(finished_client)
        if not self.finished_client(finished_client):
            return False

        return True

    def any_more_messages(self):
        pending_messages = self.receiver_handler.ammount_of_messages_in_queue()
        if pending_messages == None:
            return None
        return pending_messages > 0

    def initialize_based_on_log_finished_writing(self, log):
        #recibir un batch
        batch = None
        more_messages = self.any_more_messages()
        if more_messages == None:
            return False
        if more_messages:
            batch = self.receiver_handler.recv_batch()
            if not batch:
                print(f"[GatewayOut] Disconnected from MOM, while receiving_message")
                return False
        
            if self.is_dup_batch(batch):
                if not self.receiver_handler.ack_batch():
                    print(f"[GatewayOut] Disconnected from MOM, while acking_message")
                    return False
            else:
                self.receiver_handler.keep_batch(batch)
        
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
        try:
            self.gateway_conn.send(self.las_client_id)
            for client_id in self.pending_eof.keys():
                self.gateway_conn.send(client_id)
            self.gateway_conn.send(None)
        except Exception as e:
            print("[GatewayOut] Disconected from Gateway ", e)

    def initialize_based_on_last_execution(self):
        last_log = self.logger.read_last_log()
        print("[GatewayOut] last_log", last_log)
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

        with open(PERSISTANCE_DIR + 'log_type' + '.txt', "a") as file:
            file.write(f'{int(last_log.log_type)}\n')

        if switch[last_log.log_type](last_log):
            self.logger.clean()
            return True
        return False

def gateway_out_main(recv_conn, eof_to_receive):
    gateway_out = GatewayOut(recv_conn, eof_to_receive)
    gateway_out.start()
