from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.Book import Book
from utils.DatasetHandler import DatasetLine
from utils.Review import Review
from utils.auxiliar_functions import append_extend

FIRST_POOL = 0

class GatewayIn():
    def __init__(self, client_id, socket, next_pools, book_query_numbers, review_query_numbers):
        self.socket = socket
        self.com = None
        self.sigterm_queue = Queue()
        self.client_id = client_id
        self.book_query_numbers = book_query_numbers
        self.review_query_numbers = review_query_numbers
        self.next_pools = next_pools
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
    
    def handle_SIGTERM(self, _signum, _frame):
        self.sigterm_queue.put(True)
        print(f"\n\n [GatewayIn {self.client_id}] SIGTERM detected\n\n")
        self.close()

    def start(self):
        try:
            self.loop()
        except OSError:
            print(f"[GatewayIn {self.client_id}] Socket disconnected")
        self.close()

    def loop(self):
        while True:
            datasetlines = self.recv_dataset_line_batch()
            if datasetlines == None:
                print(f"[GatewayIn {self.client_id}] Socket disconnected")
                break
            if datasetlines.is_empty():
                if not self.send_eof():
                    print(f"[GatewayIn {self.client_id}] MOM disconnected")
                break
            if not self.send_batch_to_all_queries(datasetlines):
                print(f"[GatewayIn {self.client_id}] MOM disconnected")
                break

    def recv_dataset_line_batch(self):
        return Batch.from_socket(self.socket, DatasetLine)
    
    def send_eof(self):
        return self.com.produce_to_all_group_members(Batch.eof(self.client_id).to_bytes())
        
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
            pool = f'{query_number}.{FIRST_POOL}'
            batch = Batch(self.client_id, query_messages)
            if not self.com.produce_batch_of_messages(batch, pool, self.next_pools.shard_by_of_pool(pool)):
                return False
        return True
            

    def get_hashed_batchs(self, query_messages, query_number):
        batch = Batch(self.client_id, query_messages)
        pool_to_send = f"{query_number}.{FIRST_POOL}"
        amount_of_workers = self.com.amount_of_producer_group(pool_to_send)
        return batch.get_hashed_batchs(query_number,amount_of_workers) 

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

