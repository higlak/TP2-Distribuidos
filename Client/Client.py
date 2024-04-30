import threading
from utils.DatasetHandler import *
from utils.Book import Book
from utils.Review import Review
from utils.Batch import Batch
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE, QueryMessage, QUERY1_RESULT, QUERY2_RESULT, QUERY3_RESULT, QUERY4_RESULT, QUERY5_RESULT
import socket
import sys
import time

BATCH_SIZE = 25 #probablemente lo levantemos de la config
SERVER_PORT = 12345
STARTING_CLIENT_WAIT = 1
MAX_ATTEMPTS = 6
QUERY_RESULTS_PATH = "/data/query_results"
QUERY1_RESULTS_FIELDS = ""
QUERIES = 5

def get_file_paths():
    if len(sys.argv) != 3:
        print("[Client] Must receive exactly 2 parameter, first one the books filepath sencond one reviews filepath")
        return None, None
    return sys.argv[1] , sys.argv[2]

class Client():
    def __init__(self):
        self.client_socket = Client.connect_to_gateway()
        books_path, reviews_path = get_file_paths()
        book_reader = DatasetReader(books_path)
        review_reader = DatasetReader(reviews_path)
        if not book_reader or not review_reader:
            print(f"[Client] Reader invalid. Bookfile: {books_path}, Reviewfile: {reviews_path})")
            return None
        client_reader = ClientReader(self.client_socket, book_reader, review_reader)
        client_writer = ClientWriter(self.client_socket, Client.get_datasetWriters())
        reader_thread = threading.Thread(target=client_reader.start)
        writer_thread = threading.Thread(target=client_writer.start)
        self.threads = [reader_thread, writer_thread]
        
    @classmethod
    def get_datasetWriters(cls):
        headers = {
            QUERY1_RESULT: ['title','authors','publisher'],
            QUERY2_RESULT: ['authors'],
            QUERY3_RESULT: ['title','authors'],
            QUERY4_RESULT: ['title'],
            QUERY5_RESULT: ['title'],
        }
        writers = {}
        for i in range(QUERY1_RESULT, QUERY1_RESULT + QUERIES):
            path = QUERY_RESULTS_PATH + str(i) + '.csv'
            dw = DatasetWriter(path, headers[i])
            writers[i] = dw
        return writers


    @classmethod
    def connect_to_gateway(cls):
        i = STARTING_CLIENT_WAIT
        while True:
            try:
                print("[Client] Attempting connection")
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('gateway', SERVER_PORT))
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
        self.client_socket.close()
        
class ClientReader():
    def __init__(self, socket, book_reader, review_reader):
        self.socket = socket
        self.book_reader = book_reader
        self.review_reader = review_reader
    
    def start(self):
        self.send_all_from_dataset(BOOK_MSG_TYPE, self.book_reader)
        #send_all_from_dataset(REVIEW_MSG_TYPE, self.review_reader)
        self.send_eof()
        self.close()
    
    def send_all_from_dataset(self, object_type, reader):
        while True:
            datasetLines = reader.read_lines(BATCH_SIZE, object_type)
            if len(datasetLines) == 0:
                print("[Client] No more to send")
                return
            batch = Batch(datasetLines)
            
            self.socket.send(batch.to_bytes())
            print(f"[Client] Sent batch of {len(batch.messages)} elements")
    
    def send_eof(self):
        print("[Client] Sending EOF")
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
            print(f"\n\n {result_batch[0]}\n\n")
            print(f"\n\n {result_batch[0].msg_type}\n\n")
            self.writers[QUERY1_RESULT].append_objects(result_batch)
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