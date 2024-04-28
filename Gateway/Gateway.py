import threading
from time import sleep
import time
from CommunicationMiddleware.middleware import Communicator
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.Batch import Batch
from utils.auxiliar_functions import recv_exactly, send_all, byte_array_to_big_endian_integer
from utils.DatasetHandler import DatasetLine
from utils.Book import Book
from utils.Review import Review
import socket

GATEWAY_EXCHANGE_NAME = 'GATEWAY_EXCHANGE'

def messages_for_query1():
    messages = []
    for i in range(30):
        msg = QueryMessage(BOOK_MSG_TYPE, title=str(i), year=1995+i)
        if i%2:
            msg.categories = ['fiction']  
        messages.append(msg)
    return messages

def messages_for_query2():
    messages = []
    for i in range(30):
        msg = QueryMessage(BOOK_MSG_TYPE, title=str(i), year=1950+5*i)
        if i%2:
            msg.authors = ['Author1']  
        messages.append(msg)
    return messages

def start_listening():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('',12345))
    server_socket.listen()
    print("Server listening con ..:12345")
    client_socket, addr = server_socket.accept()
    print(f"\n\n Cliente conectado {addr}\n\n")
    return server_socket, client_socket, addr

def recv_queries_results(client_socket):
    print("[Gateway] Receiving queries results from queries subsystems")
    pass

class Gateway():
    def __init__(self, port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('',port))
        self.server_socket.listen()
        self.threads = []
        print(f"Server listening con {self.server_socket.getsockname()}")
    
    def accept_client(self):
        client_socket, addr = self.server_socket.accept()
        print(f"\n\n Client connected {addr}")
        gateway_in = GatewayIn(client_socket)
        gateway_in_thread = threading.Thread(target=gateway_in.start)
        self.threads.append(gateway_in_thread)
        
        return client_socket
    
    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join()

    def run(self):
        client_socket = self.accept_client()
        self.start_and_wait()
        client_socket.close()
        self.server_socket.close()

class GatewayIn():
    def __init__(self, client_socket):
        self.client_socket = client_socket
        while True:
            try:
                self.com = Communicator()
                break
            except:
                print("Rabbit not ready")
                sleep(1)
        self.book_query_numbers = [1,2,3,5] #p dsp sacar de var de entorno\
        self.review_query_numbers = [3,5]
    
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
            print("Fallo la lectura del socket")
            return None
        ammount_of_dataset_lines = ammount_of_dataset_lines[0]
        print(f"[Gateway] Receiving {ammount_of_dataset_lines} dataset lines from client")
        if ammount_of_dataset_lines == 0:
            print("Recibi EOF")
            return None
        datasetlines = []
        for i in range(ammount_of_dataset_lines):
            datasetline = DatasetLine.from_socket(self.client_socket)
            datasetlines.append(datasetline)
            print(datasetline)
        return datasetlines
    
    def send_eof(self):
        pass

    def object_to_query1(obj):
        if isinstance(obj, Book):
            QueryMessage.query1_from_book()
        
    def get_query_message(self, obj, query_number):
        switch = {
            1: obj.to_query1,
            2: obj.to_query2,
            3: obj.to_query3,
            5: obj.to_query5,
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
        for query_number in queries:
            query_messages = []
            for obj in objects:
                query_message = self.get_query_message(obj, query_number)
                if query_message:
                    query_messages.append(query_message)
            batch = Batch(query_messages)
            if not batch.is_empty():
                self.com.produce_message(f'{query_number}.0', batch.to_bytes())

    def get_object_from_line(self, datasetLine):
        if datasetLine.is_book():
            return Book.from_datasetline(datasetLine)
        return Review.from_datasetline(datasetLine)

def unknown_query():
    print("attempting_to_proccess unkwown query")

def main():
    gateway = Gateway(12345)
    gateway.run()
 

main()