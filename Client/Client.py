from multiprocessing import Process
from utils.DatasetHandler import *
from utils.Batch import Batch
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE, QueryMessage, query_to_query_result, query_result_headers
from utils.auxiliar_functions import get_env_list, InstanceError
from queue import Queue, Empty
import socket
import os
import sys
import time
import signal

STARTING_CLIENT_WAIT = 1
MAX_ATTEMPTS = 6

def get_file_paths():
    if len(sys.argv) != 3:
        print("[Client] Must receive exactly 2 parameter, first one the books filepath sencond one reviews filepath")
        return None, None
    return sys.argv[1] , sys.argv[2]

class Client():
    def __init__(self):
        self.threads = []
        self.signal_queue = Queue()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        try:
            queries = get_env_list("QUERIES")
            query_result_path = os.getenv("QUERY_RESULTS_PATH")
            batch_size = int(os.getenv("BATCH_SIZE"))
            server_port = int(os.getenv("SERVER_PORT"))
        except Exception as r:
            print("[Client] Error converting env vars: ", r)
            raise InstanceError
        books_path, reviews_path = get_file_paths()
        book_reader = DatasetReader(books_path)
        review_reader = DatasetReader(reviews_path)
        if not book_reader or not review_reader:
            print(f"[Client] Reader invalid. Bookfile: {books_path}, Reviewfile: {reviews_path})")
            raise InstanceError
        
        send_socket, receive_socket = Client.connect_both_ways_to_gateway(server_port, self.signal_queue)
    
        client_reader = ClientReader(send_socket, book_reader, review_reader, batch_size)
        client_writer = ClientWriter(receive_socket)
        self.threads.append(Process(target=client_reader.start))
        self.threads.append(Process(target=client_writer.start))
        
    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n Entre al sigterm\n\n")
        self.signal_queue.put(True)
        for thread in self.threads:
            thread.terminate()

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
    def connect_both_ways_to_gateway(cls, port, signal_queue):
        send_socket = Client.connect_to_gateway(port, signal_queue)
        if not send_socket:
            raise InstanceError
        receive_socket = Client.connect_to_gateway(port, signal_queue)
        if not receive_socket:
            send_socket.close()
            raise InstanceError
        return send_socket, receive_socket
        

    @classmethod
    def connect_to_gateway(cls, port, signal_queue):
        i = STARTING_CLIENT_WAIT
        while True:
            try:
                print("[Client] Attempting connection")
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('gateway', port))
                print("[Client] Client Connected to gateway")
                return client_socket
            except:
                if i > 2**MAX_ATTEMPTS:
                    print("[Client] Could not connect to Gateway. Max attempts reached")
                    return None
                print("[Client] Gateway not ready")
                try:
                    signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2

    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join() 
        
class ClientReader():
    def __init__(self, socket, book_reader, review_reader, batch_size):
        self.socket = socket
        self.book_reader = book_reader
        self.review_reader = review_reader
        self.batch_size = batch_size

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [ClientReader] SIGTERM detected \n\n")
        self.socket.close()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        try:
            self.send_all_from_dataset(BOOK_MSG_TYPE, self.book_reader)
            self.send_all_from_dataset(REVIEW_MSG_TYPE, self.review_reader)
            self.send_eof()
        except OSError:
            print("[ClientReader] Socket disconnected")
        self.close_readers()
    
    def send_all_from_dataset(self, object_type, reader):
        while True:
            datasetLines = reader.read_lines(self.batch_size, object_type)
            if len(datasetLines) == 0:
                return True
            batch = Batch(datasetLines)
            self.socket.send(batch.to_bytes())
    
    def send_eof(self):
        self.socket.send(Batch([]).to_bytes())

    def close_readers(self):
        self.book_reader.close()
        self.review_reader.close()

class ClientWriter():
    def __init__(self, socket):
        self.socket = socket
        writers = {}
        for query in ['1','2','3','4','5']:
            query_result = query_to_query_result(query)
            header = query_result_headers(query_result)
            path = "/data/query_results" + query + '.csv'
            dw = DatasetWriter(path, header)
            writers[query_result] = dw
        self.writers = writers

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [ClientWriter] SIGTERM detected\n\n")
        self.socket.close()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        while True:
            try:
                result_batch = Batch.from_socket(self.socket, QueryMessage)
                if not result_batch:
                    print("[ClientWriter] Socket disconnected")
                    break
            except OSError:
                print("[ClientWriter] Socket disconnected")
                break
            if result_batch.is_empty():
                print("[ClientWriter] Finished receiving")
                break
            self.writers[result_batch[0].msg_type].append_objects(result_batch)
        self.close()

    def close(self):
        for writer in self.writers.values():
            writer.close()
                

def main():
    try: 
        client = Client()
    except InstanceError:
        return
    client.start_and_wait()

main()