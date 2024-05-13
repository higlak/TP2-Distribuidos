import signal
from multiprocessing import Process
from CommunicationMiddleware.middleware import Communicator
from utils.NextPools import NextPools
from utils.QueryMessage import QueryMessage
from utils.Batch import Batch
from utils.auxiliar_functions import recv_exactly, send_all, get_env_list, append_extend, InstanceError
from utils.DatasetHandler import DatasetLine
from utils.Book import Book
from utils.Review import Review
import socket
import os
from queue import Queue

GATEWAY_QUEUE_NAME = 'Gateway'
QUERY_SEPARATOR = ","
FIRST_POOL = 0

class Gateway():
    def __init__(self):
        self.threads = []
        self.signal_queue_in = Queue()
        self.signal_queue_out = Queue()
        self.next_pools = NextPools.from_env()
        self.server_socket = None
        if not self.next_pools:
            raise InstanceError
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        
        try:
            port = int(os.getenv("PORT"))
            self.eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
            self.book_query_numbers = get_env_list("BOOK_QUERIES")
            self.review_query_numbers = get_env_list("REVIEW_QUERIES")
        except Exception as r:
            print(f"[Gateway] Failed converting env_vars ", r)
            raise InstanceError
        
        self.com_in, self.com_out = Gateway.connect_in_out(self.signal_queue_in, self.signal_queue_out, self.next_pools)           
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('',port))
        self.server_socket.listen()
        
        print(f"[Gateway] Listening on: {self.server_socket.getsockname()}")
     
    @classmethod   
    def connect_in_out(cls, signal_queue_in, signal_queue_out, next_pools):
        com_in = Communicator(signal_queue_in, next_pools.worker_ids())
        com_out = Communicator(signal_queue_out)
        return com_in, com_out

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [Gateway] SIGTERM detected\n\n")
        self.signal_queue_in.put(True)
        self.signal_queue_out.put(True)
        for thread in self.threads:
            thread.terminate()
        if self.server_socket:
            self.server_socket.close()

    def handle_new_client(self):
        self.threads.append(self.accept_client_receive_socket())
        self.threads.append(self.accept_client_send_socket())
        self.start_and_wait()

    def accept_client_receive_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_in = GatewayIn(client_socket, self.com_in, self.next_pools, self.book_query_numbers, self.review_query_numbers)
        gateway_in_thread = Process(target=gateway_in.start)
        return gateway_in_thread
    
    def accept_client_send_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_out = GatewayOut(client_socket, self.eof_to_receive, self.com_out)
        gateway_out_thread = Process(target=gateway_out.start)
        return gateway_out_thread
    
    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join()

    def run(self):
        while True:
            try:
                self.handle_new_client()
            except Exception as e:
                print("[Gateway] Socket disconnected \n", e)
                break
            print("[Gateway] Client disconnected. Waiting for new client...")
        
        self.close()
    
    def close(self):
        self.server_socket.close()
        self.com_in.close_connection()
        self.com_out.close_connection()
        
class GatewayOut():
    def __init__(self, socket, eof_to_receive, com):
        self.socket = socket
        self.com = com
        self.eof_to_receive = eof_to_receive

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOut] SIGTERM detected\n\n")
        self.close()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        try:
            self.loop()
        except OSError:
            print("[GatewayOut] Socket disconnected")
        self.close()

    def loop(self):
        while True:
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if not batch_bytes:
                print(f"[GatewayOut] Disconnected from MOM")
                break
            batch = Batch.from_bytes(batch_bytes)
            if batch.is_empty():
                self.eof_to_receive -= 1
                print(f"[GatewayOut] Pending EOF to receive: {self.eof_to_receive}")
                if not self.eof_to_receive:
                    print("[Gateway] No more EOF to receive. Sending EOF to client")
                    send_all(self.socket, batch.to_bytes())
                    break
            else:
                batch.keep_fields()
                print(f"[GatewayOut] Sending result to client with {batch.size()} elements")
                send_all(self.socket, batch.to_bytes())
    
    def close(self):
        self.socket.close()
    
class GatewayIn():
    def __init__(self, socket, com, next_pools, book_query_numbers, review_query_numbers):
        self.socket = socket
        self.com = com
        self.book_query_numbers = book_query_numbers
        self.review_query_numbers = review_query_numbers
        self.next_pools = next_pools
    
    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayIn] SIGTERM detected\n\n")
        self.close()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        try:
            self.loop()
        except OSError:
            print("[GatewayIn] Socket disconnected")
        self.close()

    def loop(self):
        while True:
            datasetlines = self.recv_dataset_line_batch()
            if datasetlines == None:
                print("[Gateway] Socket disconnected")
                break
            if not datasetlines:
                if not self.send_eof():
                    print("[GatewayIn] MOM disconnected")
                break
            if not self.send_batch_to_all_queries(datasetlines):
                print("[GatewayIn] MOM disconnected")
                break

    def recv_dataset_line_batch(self):
        ammount_of_dataset_lines = recv_exactly(self.socket,1)
        if ammount_of_dataset_lines == None:
            return None
        ammount_of_dataset_lines = ammount_of_dataset_lines[0]
        if ammount_of_dataset_lines == 0:
            print("[Gateway] EOF received")
            return []
        datasetlines = []
        for _ in range(ammount_of_dataset_lines):
            datasetline = DatasetLine.from_socket(self.socket)
            if not datasetline:
                print("[Gateway] Socket disconnected")
                return None
            datasetlines.append(datasetline)
        return datasetlines
    
    def send_eof(self):
        return self.com.produce_to_all_group_members(Batch([]).to_bytes())

    def object_to_query1(obj):
        if isinstance(obj, Book):
            QueryMessage.query1_from_book()
        
    def get_query_messages(self, obj, query_number):
        switch = {
            '1': obj.to_query1,
            '2': obj.to_query2,
            '3': obj.to_query3,
            '5': obj.to_query5,
        }
        
        method = switch.get(query_number, unknown_query)       
        return method()

    def send_batch_to_all_queries(self, batch):
        objects = []
        for datasetLine in batch:
            obj = self.get_object_from_line(datasetLine)
            if obj:
                objects.append(obj)
        if objects[0].is_book():
            return self.send_objects_to_queries(objects, self.book_query_numbers)
        return self.send_objects_to_queries(objects, self.review_query_numbers)
        
    def send_objects_to_queries(self, objects, queries):
        query_messages = []
        for query_number in queries:
            query_messages = []
            for obj in objects:
                query_message = self.get_query_messages(obj, query_number)
                if query_message:
                    append_extend(query_messages, query_message)
            pool = f'{query_number}.{FIRST_POOL}'
            batch = Batch(query_messages)
            if not self.com.produce_batch_of_messages(batch, pool, self.next_pools.shard_by_of_pool(pool)):
                return False
        return True
            

    def get_hashed_batchs(self, query_messages, query_number):
        batch = Batch(query_messages)
        pool_to_send = f"{query_number}.{FIRST_POOL}"
        amount_of_workers = self.com.amount_of_producer_group(pool_to_send)
        return batch.get_hashed_batchs(query_number,amount_of_workers) 

    def get_object_from_line(self, datasetLine):
        if datasetLine.is_book():
            return Book.from_datasetline(datasetLine)
        return Review.from_datasetline(datasetLine)
    
    def close(self):
        self.socket.close()

def unknown_query():
    print("[Gateway] Attempting to proccess unkwown query")

def main():
    try:
        gateway = Gateway()
    except InstanceError:
        return
    gateway.run()
 
main()