from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.Book import Book
from utils.DatasetHandler import DatasetLine
from utils.Review import Review
from utils.auxiliar_functions import append_extend, recv_exactly

FIRST_POOL = 0

class GatewayIn():
    def __init__(self, socket, com, next_pools, book_query_numbers, review_query_numbers):
        self.socket = socket
        self.com = com
        self.book_query_numbers = book_query_numbers
        self.review_query_numbers = review_query_numbers
        self.next_pools = next_pools
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
    
    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayIn] SIGTERM detected\n\n")
        self.close()

    def start(self):
        try:
            self.loop()
        except OSError:
            print("[GatewayIn] Socket disconnected")
        self.close()

    def loop(self):
        while True:
            datasetlines = self.recv_dataset_line_batch()
            if datasetlines == None:
                print("[Gateway] Socket disconnected")
                break
            if not datasetlines:
                if not self.send_eof():
                    print("[GatewayIn] MOM disconnected")
                break
            if not self.send_batch_to_all_queries(datasetlines):
                print("[GatewayIn] MOM disconnected")
                break

    def recv_dataset_line_batch(self):
        ammount_of_dataset_lines = recv_exactly(self.socket,1)
        if ammount_of_dataset_lines == None:
            return None
        ammount_of_dataset_lines = ammount_of_dataset_lines[0]
        if ammount_of_dataset_lines == 0:
            print("[Gateway] EOF received")
            return []
        datasetlines = []
        for _ in range(ammount_of_dataset_lines):
            datasetline = DatasetLine.from_socket(self.socket)
            if not datasetline:
                print("[Gateway] Socket disconnected")
                return None
            datasetlines.append(datasetline)
        return datasetlines
    
    def send_eof(self):
        return self.com.produce_to_all_group_members(Batch([]).to_bytes())
        
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
            batch = Batch(query_messages)
            if not self.com.produce_batch_of_messages(batch, pool, self.next_pools.shard_by_of_pool(pool)):
                return False
        return True
            

    def get_hashed_batchs(self, query_messages, query_number):
        batch = Batch(query_messages)
        pool_to_send = f"{query_number}.{FIRST_POOL}"
        amount_of_workers = self.com.amount_of_producer_group(pool_to_send)
        return batch.get_hashed_batchs(query_number,amount_of_workers) 

    def get_object_from_line(self, datasetLine):
        if datasetLine.is_book():
            return Book.from_datasetline(datasetLine)
        return Review.from_datasetline(datasetLine)
    
    def close(self):
        self.socket.close()
        self.com.close_connection()

def unknown_query():
    print("[Gateway] Attempting to proccess unkwown query")
    
def gateway_in_main(client_socket, next_pools, book_query_numbers, review_query_numbers):
    signal_queue = Queue()
    com = Communicator.new(signal_queue, next_pools.worker_ids())
    if not com:
        return
    gateway_in = GatewayIn(client_socket, com, next_pools, book_query_numbers, review_query_numbers)
    gateway_in.start()

