from multiprocessing import Process, Pipe
from time import sleep
from utils.Batch import AMOUNT_OF_CLIENT_ID_BYTES, Batch
from utils.DatasetHandler import DatasetReader
from utils.auxiliar_functions import get_env_list, process_has_been_started, send_all
from ClientReadWriter.ClientWriter import ClientWriter
from ClientReadWriter.ClientReader import ClientReader, CLIENTS_SENDER_ID
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
    def __init__(self, queries, query_result_path, batch_size, server_port):
        self.queries = queries
        self.query_result_path = query_result_path
        self.batch_size = batch_size
        self.server_port = server_port
        self.finished = False
        
        self.client_reader = None
        self.writer_process = None
        self.id = NO_CLIENT_ID
        self.signal_queue = Queue()
        self.client_reader_conn, self.client_conn = Pipe()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    @classmethod
    def new(cls):
        try:
            queries_str = get_env_list("QUERIES")
            queries = []
            for query in queries_str:
                queries.append(int(query))
            query_result_path = os.getenv("QUERY_RESULTS_PATH")
            batch_size = int(os.getenv("BATCH_SIZE"))
            server_port = int(os.getenv("SERVER_PORT"))
        except Exception as r:
            print("[Client] Error converting env vars: ", r)
            return None
        
        return Client(queries, query_result_path, batch_size, server_port)
    
    def connect(self):
        send_socket = Client.connect_to_gateway(self.server_port, self.signal_queue, self.id)
        if not send_socket:
            return None, None
        
        id = self.receive_id(send_socket)
        if id == None:
            return self.connect()
        self.id = id
        receive_socket = Client.connect_to_gateway(self.server_port, self.signal_queue, id)
        if not receive_socket:
            send_socket.close()
            return None, None
        
        return send_socket, receive_socket

    def create_read_writers(self, send_socket, receive_socket):
        books_path, reviews_path = get_file_paths()
        book_reader = DatasetReader(books_path)
        review_reader = DatasetReader(reviews_path)
        if not book_reader or not review_reader:
            print(f"[Client] Reader invalid. Bookfile: {books_path}, Reviewfile: {reviews_path})")
            return None, None
        
        client_reader = ClientReader(self.id, send_socket, book_reader, review_reader, self.batch_size)
        client_writer = ClientWriter(self.id, receive_socket, self.queries, self.query_result_path)
        return client_reader, client_writer
        
    def receive_id(self, socket):
        batch = Batch.from_socket(socket)
        if batch:
            print(f"[Client] Starting with client id: {batch.client_id}")
            return batch.client_id
        return None

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n Entre al sigterm\n\n")
        self.signal_queue.put(True)
        if self.writer_process != None:
            if process_has_been_started(self.writer_process):
                self.writer_process.terminate()
        self.client_reader.close()
        self.finished = True

    @classmethod
    def connect_to_gateway(cls, port, signal_queue, id):
        i = STARTING_CLIENT_WAIT
        while True:
            try:
                print("[Client] Attempting connection")
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(('gateway', port))
                print("[Client] Client Connected to gateway")
                batch = Batch.new(id, CLIENTS_SENDER_ID, [])
                send_all(client_socket, batch.to_bytes())
                return client_socket
            except OSError as e:
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

    def start(self):
        book_reading_pos, review_reading_pos, writer_finished = (0,0, False)
        while (book_reading_pos != None or review_reading_pos != None or not writer_finished) and not writer_finished:
            previouse_id = self.id
            send_socket, receive_socket = self.connect()
            if not send_socket or not receive_socket:
                break
            if self.id != previouse_id:
                book_reading_pos, review_reading_pos, writer_finished = (0,0, False)

            self.client_reader, client_writer = self.create_read_writers(send_socket, receive_socket)
            if not self.client_reader or not client_writer:
                break 
        
            self.writer_process = Process(target=client_writer.start)
            self.writer_process.start()
            book_reading_pos, review_reading_pos = self.client_reader.start(book_reading_pos, review_reading_pos)
            if book_reading_pos != None or review_reading_pos != None:
                print("Terminating writer")
                self.writer_process.terminate()
            self.writer_process.join()
            print("Joined writer")
            writer_finished = self.writer_process.exitcode
            print(f"book_reading_pos {book_reading_pos}, review_redaing_pos {review_reading_pos}, writer_finished { writer_finished}")

def main():
    client = Client.new()
    if client:
        client.start()

main()