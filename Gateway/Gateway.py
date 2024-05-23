import signal
from multiprocessing import Process, Pipe
from utils.NextPools import NextPools
from utils.auxiliar_functions import get_env_list
from GatewayInOut.GatewayIn import gateway_in_main
from GatewayInOut.GatewayOut import gateway_out_main
import socket
import os

QUERY_SEPARATOR = ","
AMOUNT_OF_IDS = 2**32
JOIN_HANDLE_POS = 1
ID_POS = 0
SERVER_SOCKET_TIMEOUT = 5

class Gateway():
    def __init__(self, port, next_pools, eof_to_receive, book_query_numbers, review_query_numbers):
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive 
        self.book_query_numbers = book_query_numbers 
        self.review_query_numbers = review_query_numbers
        self.client_handlers = {} 
        self.server_socket = None
        self.next_id = 0
        
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
            client_handler[JOIN_HANDLE_POS].terminate()
        self.gateway_out_handler.terminate()
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
        
        print(f"[Gateway] Client connected with ip: {addr[0]}")
        if addr[0] in self.client_handlers:
            id = self.client_handlers[addr[0]][ID_POS] 
            self.gateway_out_pipe.send((id, client_socket))
            self.client_handlers[addr[0]][JOIN_HANDLE_POS].start()
        else:
            id = self.get_next_id()
            gateway_in_handler = Process(target=gateway_in_main, args=[id, client_socket, self.next_pools, self.book_query_numbers, self.review_query_numbers])
            self.client_handlers[addr[0]] = (id, gateway_in_handler)
    
    def join_clients(self, blocking):
        finished = []
        for addr, client_handler in self.client_handlers.items():
            if (blocking or not client_handler[JOIN_HANDLE_POS].is_alive()) and client_handler[JOIN_HANDLE_POS].exitcode != None:
                client_handler[JOIN_HANDLE_POS].join()
                finished.append(addr)
        for addr in finished:
            del self.client_handlers[addr]

    def run(self):
        while True:
            try:
                self.handle_new_client_connection()
                self.join_clients(blocking=False)
            except Exception as e:
                print("[Gateway] Socket disconnected \n", e)
                break
        
        self.close()
    
    def close(self):
        self.join_clients(blocking=True)
        self.gateway_out_handler.join()
        self.server_socket.close()

def main():
    gateway = Gateway.new()
    if not gateway:
        return None
    gateway.run()
 
main()