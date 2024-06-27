import signal
from multiprocessing import Process, Pipe
import time
from utils.NextPools import NextPools
from utils.Batch import Batch, AMOUNT_OF_CLIENT_ID_BYTES
from utils.SenderID import SenderID
from utils.faulty import set_classes_as_faulty_if_needed, set_class_as_faulty
from utils.auxiliar_functions import get_env_list, process_has_been_started, send_all
from GatewayInOut.GatewayIn import gateway_in_main, GatewayIn, send_missed_reconections
from GatewayInOut.GatewayOut import gateway_out_main, GatewayOut, TIME_FOR_RECONNECTION
import os

QUERY_SEPARATOR = ","
AMOUNT_OF_IDS = 2**(8*AMOUNT_OF_CLIENT_ID_BYTES) - 1
JOIN_HANDLE_POS = 1
ID_POS = 0
SERVER_SOCKET_TIMEOUT = 0.5
GATEWAY_SENDER_ID = SenderID(0,0,1)
NO_CLIENT_ID = AMOUNT_OF_IDS


class Gateway():
    def __init__(self, port, next_pools, eof_to_receive, book_query_numbers, review_query_numbers):
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive 
        self.book_query_numbers = book_query_numbers 
        self.review_query_numbers = review_query_numbers
        self.client_handlers = {} 
        self.server_socket = None
        self.next_id = 0
        self.last_execution_clients = set()
        self.finished = False

        gateway_conn, self.gateway_out_conn = Pipe()
        self.gateway_out_handler = Process(target=gateway_out_main, args=[gateway_conn, self.eof_to_receive])
        self.gateway_out_handler.start()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.settimeout(SERVER_SOCKET_TIMEOUT)
        self.server_socket.bind(('',port))
        self.server_socket.listen()

        self.starting_time = time.time()
        
        print(f"[Gateway] Listening on: {self.server_socket.getsockname()}")
     
    @classmethod
    def new(cls):
        next_pools = NextPools.from_env()
        if not next_pools:
            return None
        
        try:
            port = int(os.getenv("PORT"))
            eof_to_receive = int(os.getenv("EOF_TO_RECEIVE"))
            book_query_numbers = get_env_list("BOOK_QUERIES")
            review_query_numbers = get_env_list("REVIEW_QUERIES")
        except Exception as r:
            print(f"[Gateway] Failed converting env_vars ", r)
            return None
        
        return Gateway(port, next_pools, eof_to_receive, book_query_numbers, review_query_numbers)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [Gateway] SIGTERM detected\n\n")
        self.finished = False
        self.close()

    def get_next_id(self):
        id = self.next_id
        self.next_id = (self.next_id + 1) % AMOUNT_OF_IDS
        return id

    def send_to_gateway_out(self, to_send):
        try:
            self.gateway_out_conn.send(to_send)
            return True
        except Exception as e:
            print("[Gateway] error communicating with gateway out. ", e)
        return False

    def handle_new_client_connection(self):
        try:
            client_socket, addr = self.server_socket.accept()
        except socket.timeout:
            return True
        except OSError as e:
            print("[Gateway] Error accepting connections")
            return False

        client_id = self.rcv_client_id(client_socket)
        if client_id == None:
            client_socket.close()
            return True

        if client_id == NO_CLIENT_ID:
            if not self.handle_receiving_connection(client_socket, client_id):
                return False
            print("[Gateway] New client recv conn")
        else:
            if client_id in self.client_handlers:
                if not self.send_to_gateway_out((client_id, client_socket)):
                    return False
                #if not process_has_been_started(self.client_handlers[client_id]):
                #    self.client_handlers[client_id].start()
            else:
                if not self.reconected_too_late():
                    if not self.handle_receiving_connection(client_socket, client_id):
                        return False
                    print("[Gateway] New client recv conn")
                    try:
                        print("[Gateway] Last execution clients", self.last_execution_clients)
                        self.last_execution_clients.remove(client_id)
                    except:
                        client_socket.close()
                        print("[Gateway] I closed it, so sad")
                else:
                    if not self.handle_receiving_connection(client_socket, NO_CLIENT_ID):
                        return False
                    print("[Gateway] New client recv conn")

        print(f"[Gateway] Client {client_id} connected")
        return True
    
    def handle_receiving_connection(self, client_socket, client_id):
        client_id = self.send_client_id(client_socket, client_id)
        if client_id == None:
            return False
        gateway_in_handler = Process(target=gateway_in_main, args=[client_id, client_socket, self.next_pools, self.book_query_numbers, self.review_query_numbers])
        gateway_in_handler.daemon = True
        self.client_handlers[client_id] = gateway_in_handler
        return True
    
    def send_client_id(self, sock, id=NO_CLIENT_ID):
        if id == NO_CLIENT_ID:
            id = self.get_next_id()
        batch = Batch.new(id, GATEWAY_SENDER_ID, [])
        try:
            send_all(sock, batch.to_bytes())
        except OSError as e:
            print("[Gateway] Disconected sending id")
            return None
        print(f"[Gateway] Assigning client id {id}")
        return id

    def rcv_client_id(self, sock):
        id_batch = Batch.from_socket(sock)
        if id_batch:
            return id_batch.client_id
        print("[Gateway] Failed to receive client_id")
        return None

    def join_clients(self, blocking):
        finished = []
        process_failed = False
        for id, client_handler in self.client_handlers.items():
            if (blocking or not client_handler.is_alive()) and process_has_been_started(client_handler):
                print(f"[gateway] joining client {id}")
                client_handler.join()
                if client_handler.exitcode != 0:
                    print(f"[Gateway] client {id} paniced")
                    process_failed = True
                finished.append(id)
        for id in finished:
            self.client_handlers.pop(id)
        if not self.gateway_out_handler.is_alive():
            process_failed = True
        return not process_failed

    def get_last_execution_clients(self):
        try:
            self.next_id = self.gateway_out_conn.recv() + 1
            client_id = self.gateway_out_conn.recv()
            while client_id != None:
                self.last_execution_clients.add(client_id)
                client_id = self.gateway_out_conn.recv()
        except Exception as e:
            print("[Gateway] Disconected from_gateway out")
        self.starting_time = time.time()

    def reconected_too_late(self):
        return time.time() > self.starting_time + TIME_FOR_RECONNECTION

    def handle_last_execution_client_reconection(self):
        if not self.last_execution_clients:
            return True
        print(f"not yet: {time.time()} > {self.starting_time} + {TIME_FOR_RECONNECTION} = {self.starting_time + TIME_FOR_RECONNECTION} => {self.reconected_too_late()}")
        if self.reconected_too_late():
            for id in self.last_execution_clients:
                if not self.send_to_gateway_out((id, None)):
                    return False
            if not self.send_to_gateway_out((NO_CLIENT_ID, None)):
                return False
            sender_handler = Process(target=send_missed_reconections, args=[self.last_execution_clients, self.next_pools, self.book_query_numbers, self.review_query_numbers])
            sender_handler.daemon = True
            self.client_handlers[NO_CLIENT_ID] = sender_handler
            
            self.last_execution_clients = []
        #meter al sistema los eof(se la bancan los workers si es lo unico que tienen de un cliente?)
        return True
    
    def start_gateway_ins(self):
        while self.gateway_out_conn.poll():
            client_id = self.gateway_out_conn.recv()
            print("starting id ", client_id)
            if not process_has_been_started(self.client_handlers[client_id]):
                self.client_handlers[client_id].start()

    def run(self):
        self.get_last_execution_clients()
        while not self.finished:
            if not self.handle_new_client_connection():
                break
            if not self.handle_last_execution_client_reconection():
                break
            self.start_gateway_ins()
            if not self.join_clients(blocking=False):
                break

        self.close()
        if not self.finished:
            print("[Gateway] me fui sin sigterm")
    
    def close(self):
        if self.finished:
            return
        for id, client_handler in self.client_handlers.items():
            if client_handler.is_alive():
                print("[gateway] terminating client ", id)
                client_handler.terminate()
        
        print("[gateway] terminating gateway out")
        self.gateway_out_handler.terminate()
        if self.server_socket:
            self.server_socket.close()
        print("[gateway] Joining gateway ins")
        self.join_clients(blocking=True)
        print("[gateway] Joining gateway out")
        self.gateway_out_handler.join()
        self.server_socket.close()
        print("gateway closed everything")

def main():
    #set_classes_as_faulty_if_needed([GatewayIn, GatewayOut])
    #set_class_as_faulty(Gateway, True)
    gateway = Gateway.new()
    if not gateway:
        return None
    gateway.run()
 
main()