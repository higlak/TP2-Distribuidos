import threading
from CommunicationMiddleware.middleware import Communicator
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.Batch import Batch
from utils.auxiliar_functions import recv_exactly, send_all, byte_array_to_big_endian_integer, get_env_list
from utils.DatasetHandler import DatasetLine
from utils.Book import Book
from utils.Review import Review
import socket
import os

GATEWAY_QUEUE_NAME = 'Gateway'
QUERY_SEPARATOR = ","
FIRST_POOL = 0

def recv_queries_results(client_socket):
    print("[Gateway] Receiving queries results from queries subsystems")
    pass

class Gateway():
    def __init__(self):
        try:
            port = int(os.getenv("PORT"))
            eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
            forward_to = get_env_list("FORWARD_TO")
            next_pool_workers = get_env_list("NEXT_POOL_WORKERS") 
            next_pool_queues = []
            for i in range(len(next_pool_workers)):
                next_pool_queues.append([f'{forward_to[i]}.{j}' for j in range(int(next_pool_workers[i]))])

        except Exception as r:
            print(f"[Gateway] Failed converting env_vars ", r)
            return None
        
        print("Group: ", dict(zip(forward_to, next_pool_queues)))
        com_in = Communicator(dict(zip(forward_to, next_pool_queues)))
        com_out = Communicator()
        if not com_in or not com_out:
            return None
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('',port))
        self.server_socket.listen()
        print(f"[Gateway] Listening on: {self.server_socket.getsockname()}")
        self.client_socket, self.threads = Gateway.accept_client(self.server_socket, com_in, com_out, eof_to_receive)
    
    @classmethod
    def accept_client(self, server_socket, com_in, com_out, eof_to_receive):
        client_socket, addr = server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_in = GatewayIn(client_socket, com_in)
        gateway_out = GatewayOut(client_socket, eof_to_receive, com_out)
        gateway_in_thread = threading.Thread(target=gateway_in.start)
        gateway_out_thread = threading.Thread(target=gateway_out.start)
        return client_socket, [gateway_in_thread, gateway_out_thread]
    
    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join()

    def run(self):
        self.start_and_wait()
        self.client_socket.close()
        self.server_socket.close()

class GatewayOut():
    def __init__(self, client_socket, eof_to_receive, com):
        self.client_socket = client_socket
        self.com = com
        self.eof_to_receive = eof_to_receive

    def start(self):
        self.loop()

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
                    print("[Gateway] No more EOF to receive")
                    send_all(self.client_socket, batch.to_bytes())
                    break
            
            print("[Gateway] Sending result to client")
            send_all(self.client_socket, batch_bytes)

class GatewayIn():
    def __init__(self, client_socket, com):
        self.client_socket = client_socket
        self.com = com
        self.book_query_numbers = get_env_list("BOOK_QUERIES")
        self.review_query_numbers = get_env_list("REVIEW_QUERIES")
    
    def start(self):
        self.loop()

    def loop(self):
        while True:
            datasetlines = self.recv_dataset_line_batch()
            if not datasetlines:
                self.send_eof()
                break
            self.send_batch_to_all_queries(datasetlines)
    
    def recv_dataset_line_batch(self):
        ammount_of_dataset_lines = recv_exactly(self.client_socket,1)
        if ammount_of_dataset_lines == None:
            print("[Gateway] Socket read failed")
            return None
        ammount_of_dataset_lines = ammount_of_dataset_lines[0]
        print(f"[Gateway] Receiving {ammount_of_dataset_lines} dataset lines from client")
        if ammount_of_dataset_lines == 0:
            print("[Gateway] EOF received")
            return None
        datasetlines = []
        for _ in range(ammount_of_dataset_lines):
            datasetline = DatasetLine.from_socket(self.client_socket)
            datasetlines.append(datasetline)
        return datasetlines
    
    def send_eof(self):
        for query in set(self.review_query_numbers + self.book_query_numbers): #p capas cambiar esto 
            print(f"[Gateway] Sending EOF to query {query}")
            self.com.produce_message_n_times(Batch([]).to_bytes(), f'{query}.0', 2)

    def object_to_query1(obj):
        if isinstance(obj, Book):
            QueryMessage.query1_from_book()
        
    def get_query_message(self, obj, query_number):
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
                query_message = self.get_query_message(obj, query_number)
                if query_message:
                    query_messages.append(query_message)
            batch = Batch(query_messages)
            if not batch.is_empty():
                self.com.produce_message(batch.to_bytes(), f'{query_number}.{FIRST_POOL}')

    def get_object_from_line(self, datasetLine):
        if datasetLine.is_book():
            return Book.from_datasetline(datasetLine)
        return Review.from_datasetline(datasetLine)

def unknown_query():
    print("[Gateway] Attempting to proccess unkwown query")

def main():
    gateway = Gateway()
    if gateway == None:
        return
    gateway.run()
 

main()