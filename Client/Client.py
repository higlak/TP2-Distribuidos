import threading
from utils.DatasetHandler import *
from utils.Book import Book
from utils.Review import Review
from utils.Batch import Batch
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE, QueryMessage, QUERY1_RESULT, QUERY2_RESULT, QUERY3_RESULT, QUERY4_RESULT, QUERY5_RESULT, query_to_query_result, query_result_headers
from utils.auxiliar_functions import get_env_list
import socket
import os
import sys
import time

STARTING_CLIENT_WAIT = 1
MAX_ATTEMPTS = 6

def get_file_paths():
    if len(sys.argv) != 3:
        print("[Client] Must receive exactly 2 parameter, first one the books filepath sencond one reviews filepath")
        return None, None
    return sys.argv[1] , sys.argv[2]

class Client():
    def __init__(self):
        try:
            queries = get_env_list("QUERIES")
            query_result_path = os.getenv("QUERY_RESULTS_PATH")
            batch_size = int(os.getenv("BATCH_SIZE"))
            server_port = int(os.getenv("SERVER_PORT"))
        except Exception as r:
            print("[Client] Error converting env vars: ", r)
            return None
        self.send_socket = Client.connect_to_gateway(server_port)
        self.receive_socket = Client.connect_to_gateway(server_port)
        books_path, reviews_path = get_file_paths()
        book_reader = DatasetReader(books_path)
        review_reader = DatasetReader(reviews_path)
        if not book_reader or not review_reader:
            print(f"[Client] Reader invalid. Bookfile: {books_path}, Reviewfile: {reviews_path})")
            return None
            
        client_reader = ClientReader(self.send_socket, book_reader, review_reader, batch_size)
        client_writer = ClientWriter(self.receive_socket, Client.get_datasetWriters(queries, query_result_path))
        reader_thread = threading.Thread(target=client_reader.start)
        writer_thread = threading.Thread(target=client_writer.start)
        self.threads = [reader_thread, writer_thread]
        
    @classmethod
    def get_datasetWriters(cls, queries, query_result_path):
        writers = {}
        for query in queries:
            query_result = query_to_query_result(query)
            header = query_result_headers(query_result)
            path = query_result_path + query + '.csv'
            dw = DatasetWriter(path, header)
            writers[query_result] = dw
        return writers

    @classmethod
    def connect_to_gateway(cls, port):
        i = STARTING_CLIENT_WAIT
        while True:
            try:
                print("[Client] Attempting connection")
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('gateway', port))
                print("[Client] Client Connected to gatewy")
                return client_socket
            except:
                if i > 2**MAX_ATTEMPTS:
                    print("[Client] Could not connect to Gateway. Max attempts reached")
                    return None
                print("[Client] Gateway not ready")
                time.sleep(i) 
                i *= 2

    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join()
        self.close()
        
    def close(self):
        self.send_socket.close()
        self.receive_socket.close()
        
class ClientReader():
    def __init__(self, socket, book_reader, review_reader, batch_size):
        self.socket = socket
        self.book_reader = book_reader
        self.review_reader = review_reader
        self.batch_size = batch_size
    
    def start(self):
        self.send_all_from_dataset(BOOK_MSG_TYPE, self.book_reader)
        #send_all_from_dataset(REVIEW_MSG_TYPE, self.review_reader)
        self.send_eof()
        self.close()
    
    def send_all_from_dataset(self, object_type, reader):
        while True:
            datasetLines = reader.read_lines(self.batch_size, object_type)
            if len(datasetLines) == 0:
                return
            batch = Batch(datasetLines)
            self.socket.send(batch.to_bytes())
    
    def send_eof(self):
        self.socket.send(Batch([]).to_bytes())

    def close(self):
        self.book_reader.close()
        self.review_reader.close()

class ClientWriter():
    def __init__(self, socket, writers):
        self.socket = socket
        self.writers = writers

    def start(self):
        while True:
            result_batch = Batch.from_socket(self.socket, QueryMessage)
            if result_batch.is_empty():
                print("Finished receiving")
                break
            self.writers[result_batch[0].msg_type].append_objects(result_batch)
        self.close()

    def close(self):
        for writer in self.writers.values():
            writer.close()
                

def main():
    client = Client()
    if not client:
        return
    client.start_and_wait()

main()