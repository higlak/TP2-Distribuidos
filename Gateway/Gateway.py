import signal
from multiprocessing import Process, Pipe
from utils.NextPools import NextPools
from utils.Batch import Batch, AMOUNT_OF_CLIENT_ID_BYTES
from utils.auxiliar_functions import get_env_list, send_all
from GatewayInOut.GatewayIn import gateway_in_main
from GatewayInOut.GatewayOut import gateway_out_main
from utils.HealthcheckReceiver import HealthcheckReceiver

import socket
import os

QUERY_SEPARATOR = ","
AMOUNT_OF_IDS = 2**(8*AMOUNT_OF_CLIENT_ID_BYTES) - 1
JOIN_HANDLE_POS = 1
ID_POS = 0
SERVER_SOCKET_TIMEOUT = 5
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
        
        self.healthcheck_receiver_thread = Process(target=handle_waker_leader)
        self.healthcheck_receiver_thread.start()

        recv_conn, send_conn = Pipe(False)
        self.gateway_out_pipe = send_conn
        self.gateway_out_handler = Process(target=gateway_out_main, args=[recv_conn, self.eof_to_receive])
        self.gateway_out_handler.start()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.settimeout(SERVER_SOCKET_TIMEOUT)
        self.server_socket.bind(('',port))
        self.server_socket.listen()
        
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
        for client_handler in self.client_handlers.values():
            client_handler.terminate()
        self.gateway_out_handler.terminate()
        self.healthcheck_receiver_thread.terminate()
        if self.server_socket:
            self.server_socket.close()

    def get_next_id(self):
        id = self.next_id
        self.next_id = (self.next_id + 1) % AMOUNT_OF_IDS
        return id

    def handle_new_client_connection(self):
        try:
            client_socket, addr = self.server_socket.accept()
        except socket.timeout:
            return

        client_id = self.rcv_client_id(client_socket)
        if client_id == None:
            return

        if client_id == NO_CLIENT_ID:
            client_id = self.send_next_client_id(client_socket)
            gateway_in_handler = Process(target=gateway_in_main, args=[client_id, client_socket, self.next_pools, self.book_query_numbers, self.review_query_numbers])
            self.client_handlers[client_id] = gateway_in_handler
        else:
            self.gateway_out_pipe.send((client_id, client_socket))
            self.client_handlers[client_id].start()

        print(f"[Gateway] Client {client_id} connected")
    
    def send_next_client_id(self, sock):
        id = self.get_next_id()
        batch = Batch(id, [])
        send_all(sock, batch.to_bytes())
        print(f"[Gateway] Assigning client id {id}")
        return id

    def rcv_client_id(self, sock):
        id_batch = Batch.from_socket(sock)
        if id_batch:
            return id_batch.client_id
        return None

    def join_clients(self, blocking):
        finished = []
        for id, client_handler in self.client_handlers.items():
            if (blocking or not client_handler.is_alive()) and client_handler.exitcode != None:
                client_handler.join()
                finished.append(id)
        for id in finished:
            self.client_handlers.pop(id)

    def run(self):
        while True:
            try:
                self.handle_new_client_connection()
                self.join_clients(blocking=False)
            except Exception:
                print("[Gateway] Socket disconnected \n")
                break
        
        self.close()
    
    def close(self):
        self.join_clients(blocking=True)
        self.gateway_out_handler.join()
        self.healthcheck_receiver_thread.join()
        self.server_socket.close()

def handle_waker_leader():
    try:            
        healthcheck_receiver = HealthcheckReceiver('Gateway')
        healthcheck_receiver.start()

    except Exception as e:
        print(f"[Gateway] Socket disconnected: {e} \n")
        return
    
def main():
    gateway = Gateway.new()
    if not gateway:
        return None
    gateway.run()
 
main()