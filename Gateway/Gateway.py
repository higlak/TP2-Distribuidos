import pprint
import threading
from CommunicationMiddleware.middleware import Communicator
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.Batch import Batch
from utils.auxiliar_functions import recv_exactly, send_all, byte_array_to_big_endian_integer, get_env_list, append_extend
from utils.DatasetHandler import DatasetLine
from utils.Book import Book
from utils.Review import Review
import socket
import os

GATEWAY_QUEUE_NAME = 'Gateway'
QUERY_SEPARATOR = ","
FIRST_POOL = 0

class Gateway():
    def __init__(self):
        try:
            port = int(os.getenv("PORT"))
            self.eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
            forward_to = get_env_list("FORWARD_TO")
            next_pool_workers = get_env_list("NEXT_POOL_WORKERS") 
            next_pool_queues = []
            for i in range(len(next_pool_workers)):
                next_pool_queues.append([f'{forward_to[i]}.{j}' for j in range(int(next_pool_workers[i]))])

        except Exception as r:
            print(f"[Gateway] Failed converting env_vars ", r)
            return None
        
        self.com_in = Communicator(dict(zip(forward_to, next_pool_queues)))
        self.com_out = Communicator()
        if not self.com_in or not self.com_out:
            return None
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('',port))
        self.server_socket.listen()
        print(f"[Gateway] Listening on: {self.server_socket.getsockname()}")
        
    def handle_new_client(self):
        receive_thread = self.accept_client_receive_socket()
        send_thread = self.accept_client_send_socket()
        threads = [receive_thread, send_thread]
        self.start_and_wait(threads)

    def accept_client_receive_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_in = GatewayIn(client_socket, self.com_in)
        gateway_in_thread = threading.Thread(target=gateway_in.start)
        return gateway_in_thread
    
    def accept_client_send_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_out = GatewayOut(client_socket, self.eof_to_receive, self.com_out)
        gateway_out_thread = threading.Thread(target=gateway_out.start)
        return gateway_out_thread
    
    def start_and_wait(self, threads):
        for thread in threads:
            thread.start()
        for handle in threads:
            handle.join()

    def run(self):
        while True:
            self.handle_new_client()
            print("[Gateway] Client disconnected. Waiting for new client...")

class GatewayOut():
    def __init__(self, socket, eof_to_receive, com):
        self.socket = socket
        self.com = com
        self.eof_to_receive = eof_to_receive

    def start(self):
        self.loop()
        self.close()

    def loop(self):
        while True:
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if batch_bytes == None:
                print(f"[Gateway] Error while consuming results")
                break
            batch = Batch.from_bytes(batch_bytes)
            if batch.is_empty():
                self.eof_to_receive -= 1
                print(f"[Gateway] Pending EOF to receive: {self.eof_to_receive}")
                if not self.eof_to_receive:
                    print("[Gateway] No more EOF to receive. Sending EOF to client")
                    send_all(self.socket, batch.to_bytes())
                    break
            else:
                batch.keep_fields()
                print(f"[Gateway] Sending result to client with {batch.size()} elements")
                send_all(self.socket, batch.to_bytes())
    
    def close(self):
        self.socket.close()
    
class GatewayIn():
    def __init__(self, socket, com):
        self.socket = socket
        self.com = com
        self.book_query_numbers = get_env_list("BOOK_QUERIES")
        self.review_query_numbers = get_env_list("REVIEW_QUERIES")
    
    def start(self):
        self.loop()
        self.close()

    def loop(self):
        while True:
            datasetlines = self.recv_dataset_line_batch()
            if not datasetlines:
                self.send_eof()
                break
            self.send_batch_to_all_queries(datasetlines)
        
    def recv_dataset_line_batch(self):
        ammount_of_dataset_lines = recv_exactly(self.socket,1)
        if ammount_of_dataset_lines == None:
            print("[Gateway] Socket read failed")
            return None
        ammount_of_dataset_lines = ammount_of_dataset_lines[0]
        if ammount_of_dataset_lines == 0:
            print("[Gateway] EOF received")
            return None
        datasetlines = []
        for _ in range(ammount_of_dataset_lines):
            datasetline = DatasetLine.from_socket(self.socket)
            datasetlines.append(datasetline)
        return datasetlines
    
    def send_eof(self):
        self.com.produce_to_all_group_members(Batch([]).to_bytes())

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
            group = f'{query_number}.{FIRST_POOL}'
            hashed_batchs = self.get_hashed_batchs(query_messages, query_number)
            for worker_to_send, batch in hashed_batchs.items():
                if not batch.is_empty():
                    self.com.produce_message(batch.to_bytes(), group, worker_to_send)

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
    gateway = Gateway()
    if gateway == None:
        return
    gateway.run()
 
main()