from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.Book import Book
from utils.DatasetHandler import DatasetLine
from utils.Review import Review
from utils.auxiliar_functions import append_extend, send_all
from utils.SenderID import SenderID

FIRST_POOL = 0
ACK_MESSAGE_BYTES = bytearray([0])

class GatewayIn():
    def __init__(self, client_id, socket, next_pools, book_query_numbers, review_query_numbers):
        self.socket = socket
        self.sigterm_queue = Queue()
        self.com = None
        self.finished = False
        
        self.pending_eof = 1
        self.client_id = client_id
        self.id = SenderID(0,0,client_id)
        self.book_query_numbers = book_query_numbers
        self.review_query_numbers = review_query_numbers
        self.next_pools = next_pools
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
    
    def handle_SIGTERM(self, _signum, _frame):
        self.sigterm_queue.put(True)
        self.finished = True
        print(f"\n\n [GatewayIn {self.client_id}] SIGTERM detected\n\n")
        self.close()

    def start(self):
        try:
            self.loop()
        except OSError:
            print(f"[GatewayIn {self.client_id}] Socket disconnected")
        self.close()

    def loop(self):
        while self.pending_eof:
            datasetlines = self.recv_dataset_line_batch()
            if not datasetlines:
                break
            print(f"[GatewayIn {self.client_id}] Received {datasetlines.seq_num}")
            if not self.process_datasetlines(datasetlines):
                break
            if not self.ack_message():
                break

    def ack_message(self):
        try:
            send_all(self.socket, ACK_MESSAGE_BYTES)
        except OSError as e:
            print(f"[GatewayIn {self.client_id}] Disconected from client, {e}")
            if not self.finished:
                self.send_eof(Batch.eof(self.client_id))
            return False
        return True

    def process_datasetlines(self, datasetlines):
        if datasetlines == None:
            print(f"[GatewayIn {self.client_id}] Socket disconnected")
            return False
        datasetlines.sender_id = self.id
        if datasetlines.is_empty():
            if not self.send_eof(datasetlines):
                print(f"[GatewayIn {self.client_id}] MOM disconnected")
            return False
        if not self.send_batch_to_all_queries(datasetlines):
            print(f"[GatewayIn {self.client_id}] MOM disconnected")
            return False
        return True

    def recv_dataset_line_batch(self):
        batch = Batch.from_socket(self.socket, DatasetLine)
        if not batch:
            print(f"[GatewayIn {self.client_id}] Disconected from client while receiving batch")
            if not self.finished:
                self.send_eof(Batch.eof(self.client_id, self.id))
            return None
        return batch
    
    def send_eof(self, batch):
        self.pending_eof -= 1
        return self.com.produce_to_all_group_members(batch.to_bytes())
        
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
            return self.send_objects_to_queries(objects, self.book_query_numbers, batch.seq_num)
        return self.send_objects_to_queries(objects, self.review_query_numbers, batch.seq_num)
        
    def send_objects_to_queries(self, objects, queries, seq_num):
        query_messages = []
        for query_number in queries:
            query_messages = []
            for obj in objects:
                query_message = self.get_query_messages(obj, query_number)
                if query_message:
                    append_extend(query_messages, query_message)
            pool = f'{query_number}.{FIRST_POOL}'
            batch = Batch(self.client_id, self.id, seq_num, query_messages)
            if not self.com.produce_batch_of_messages(batch, pool, self.next_pools.shard_by_of_pool(pool)):
                return False
        return True

    def get_object_from_line(self, datasetLine):
        if datasetLine.is_book():
            return Book.from_datasetline(datasetLine)
        return Review.from_datasetline(datasetLine)
    
    def connect(self):
        self.com = Communicator.new(self.sigterm_queue, self.next_pools.worker_ids())
        if not self.com:
            return False
        return True

    def close(self):
        self.socket.close()
        self.com.close_connection()

def unknown_query():
    print("[Gateway] Attempting to proccess unkwown query")
    
def gateway_in_main(client_id, client_socket, next_pools, book_query_numbers, review_query_numbers):
    print("\n\nStarting gatewain in\n\n")
    gateway_in = GatewayIn(client_id, client_socket, next_pools, book_query_numbers, review_query_numbers)
    if gateway_in.connect():
        gateway_in.start()

