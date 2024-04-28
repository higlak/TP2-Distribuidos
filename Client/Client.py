from utils.DatasetHandler import *
from utils.Book import Book
from utils.Review import Review
from utils.Batch import Batch
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE
import socket
import sys
import time

BATCH_SIZE = 25 #probablemente lo levantemos de la config
SERVER_PORT = 12345

def get_file_paths():
    if len(sys.argv) != 3:
        print("Must receive exactly 2 parameter, first one the books filepath sencond one reviews filepath")
        return None, None
    return sys.argv[1] , sys.argv[2]

def send_all_from_dataset(reader, object_type, client_socket):
    while True:
        datasetLines = reader.read_lines(BATCH_SIZE, object_type)
        if len(datasetLines) == 0:
            print("[Client] No more to send")
            return
        batch = Batch(datasetLines)
        
        client_socket.send(batch.to_bytes())
        print(f"[Client] Sent batch of {len(batch.messages)} elements")

def connect_to_gateway():
    while True:
        try:
            print("Attempting connection")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(('gateway', SERVER_PORT))
            print("[Cliente] Client Connected to gatewy")
            return client_socket
        except:
            print("Gateway_not ready")
        time.sleep(1) #exponential_backof

def send_eof(client_socket):
    print("Send Eof")
    client_socket.send(Batch([]).to_bytes())


def main():
    books_path, reviews_path = get_file_paths()
    book_reader = DatasetReader(books_path)
    reviews_reader = DatasetReader(reviews_path)
    if not book_reader or not reviews_reader:
        print(f"Exited invalid one ({book_reader},{reviews_reader})")
        return

    client_socket = connect_to_gateway()
    send_all_from_dataset(book_reader, BOOK_MSG_TYPE, client_socket)
    #send_all_from_dataset(book_reader, REVIEW_MSG_TYPE, client_socket)
    send_eof(client_socket)
    client_socket.close()

main()