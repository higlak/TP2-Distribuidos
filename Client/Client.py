from multiprocessing import Process
from time import sleep
from utils.Batch import AMOUNT_OF_CLIENT_ID_BYTES, Batch
from utils.DatasetHandler import DatasetReader
from utils.auxiliar_functions import get_env_list, send_all
from ClientReadWriter.ClientWriter import ClientWriter
from ClientReadWriter.ClientReader import ClientReader
from queue import Queue, Empty
import socket
import os
import sys
import signal

STARTING_CLIENT_WAIT = 1
MAX_ATTEMPTS = 6
NO_CLIENT_ID = 2**(8*AMOUNT_OF_CLIENT_ID_BYTES) - 1

def get_file_paths():
    if len(sys.argv) != 3:
        print("[Client] Must receive exactly 2 parameter, first one the books filepath sencond one reviews filepath")
        return None, None
    
    if os.getenv("A"):
        print("\n\nTengo solo prueba\n\n")
        return "/data/test.csv" , "/data/test.csv"
    else:
        print("\n\nNope\n\n")
    
    return sys.argv[1] , sys.argv[2]

class Client():
    def __init__(self):
        self.threads = []
        self.signal_queue = Queue()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    @classmethod
    def new(cls):
        try:
            queries = get_env_list("QUERIES")
            query_result_path = os.getenv("QUERY_RESULTS_PATH")
            batch_size = int(os.getenv("BATCH_SIZE"))
            server_port = int(os.getenv("SERVER_PORT"))
        except Exception as r:
            print("[Client] Error converting env vars: ", r)
            return None
        
        client = Client()
        send_socket, receive_socket, id = client.connect(server_port)
        if not send_socket or not receive_socket or id == None:
            return None

        if not client.create_read_writers(id, send_socket, receive_socket, queries, query_result_path, batch_size):
            return None
        return client
    
    def connect(self,  server_port):
        send_socket = Client.connect_to_gateway(server_port, self.signal_queue, NO_CLIENT_ID)
        if not send_socket:
            return None, None, None
        
        id = self.receive_id(send_socket)
        if id == None:
            return None, None, None
        
        receive_socket = Client.connect_to_gateway(server_port, self.signal_queue, id)
        if not receive_socket:
            send_socket.close()
            return None, None, None
        
        return send_socket, receive_socket, id

    def create_read_writers(self, id, send_socket, receive_socket, queries, query_result_path, batch_size):
        books_path, reviews_path = get_file_paths()
        book_reader = DatasetReader(books_path)
        review_reader = DatasetReader(reviews_path)
        if not book_reader or not review_reader:
            print(f"[Client] Reader invalid. Bookfile: {books_path}, Reviewfile: {reviews_path})")
            return False
        
        client_reader = ClientReader(id, send_socket, book_reader, review_reader, batch_size)
        client_writer = ClientWriter(id, receive_socket, queries, query_result_path)
        self.threads.append(Process(target=client_reader.start))
        self.threads.append(Process(target=client_writer.start))
        return True
        
    def receive_id(self, socket):
        batch = Batch.from_socket(socket)
        if batch:
            print(f"[Client] Starting with client id: {batch.client_id}")
            return batch.client_id
        return None

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n Entre al sigterm\n\n")
        self.signal_queue.put(True)
        for thread in self.threads:
            thread.terminate()

    @classmethod
    def connect_to_gateway(cls, port, signal_queue, id):
        i = STARTING_CLIENT_WAIT
        while True:
            try:
                print("[Client] Attempting connection")
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('gateway', port))
                print("[Client] Client Connected to gateway")
                batch = Batch(id, [])
                send_all(client_socket, batch.to_bytes())
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

def main():
    client = Client.new()
    if client:
        client.start_and_wait()

main()