import signal
from multiprocessing import Process
from utils.NextPools import NextPools
from utils.auxiliar_functions import get_env_list, InstanceError
from GatewayInOut.GatewayIn import gateway_in_main
from GatewayInOut.GatewayOut import gateway_out_main
import socket
import os

QUERY_SEPARATOR = ","

class Gateway():
    def __init__(self, port, next_pools, eof_to_receive, book_query_numbers, review_query_numbers):
        self.next_pools = next_pools
        self.eof_to_receive = eof_to_receive 
        self.book_query_numbers = book_query_numbers 
        self.review_query_numbers = review_query_numbers
        self.threads = []
        self.server_socket = None
        
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
        self.signal_queue_in.put(True)
        self.signal_queue_out.put(True)
        for thread in self.threads:
            thread.terminate()
        if self.server_socket:
            self.server_socket.close()

    def handle_new_client(self):
        self.threads.append(self.accept_client_receive_socket())
        self.threads.append(self.accept_client_send_socket())
        self.start_and_wait()
    
    def accept_client_receive_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_in_thread = Process(target=gateway_in_main, args=[client_socket, self.next_pools, self.book_query_numbers, self.review_query_numbers])
        return gateway_in_thread
    
    def accept_client_send_socket(self):
        client_socket, addr = self.server_socket.accept()
        print(f"[Gateway] Client connected with address: {addr}")
        gateway_out_thread = Process(target=gateway_out_main, args=[client_socket, self.eof_to_receive])
        return gateway_out_thread
    
    def start_and_wait(self):
        for thread in self.threads:
            thread.start()
        for handle in self.threads:
            handle.join()

    def run(self):
        while True:
            try:
                self.handle_new_client()
            except Exception as e:
                print("[Gateway] Socket disconnected \n", e)
                break
            print("[Gateway] Client disconnected. Waiting for new client...")
        
        self.close()
    
    def close(self):
        self.server_socket.close()

def main():
    gateway = Gateway.new()
    if not gateway:
        return None
    gateway.run()
 
main()