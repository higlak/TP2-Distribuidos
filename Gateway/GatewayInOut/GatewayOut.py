from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.auxiliar_functions import send_all

GATEWAY_QUEUE_NAME = 'Gateway'
PENDING_EOF_POS = 1
CLIENT_SOCKET_POS = 0

class GatewayOut():
    def __init__(self, com, recv_conn, eof_to_receive):
        self.clients = {}
        self.com = com
        self.eof_to_receive = eof_to_receive
        self.recv_clients = recv_conn
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOut] SIGTERM detected\n\n")
        self.close()

    def start(self):
        try:
            self.loop()
        except OSError:
            print("[GatewayOut] Socket disconnected")
        self.close()

    def loop(self):
        while True:
            batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
            if not batch_bytes:
                print(f"[GatewayOut] Disconnected from MOM")
                break

            self.get_new_clients()
            self.proccess_message(batch_bytes)

    def get_new_clients(self):
        while self.recv_clients.poll():
            id, client_socket = self.recv_conn.recv()
            print(f"[GatewatOut] Received new client with id: {id}")
            self.clients[id] = (client_socket, self.eof_to_receive)
    
    def proccess_message(self, batch_bytes):
        batch = Batch.from_bytes(batch_bytes)
        client_id = batch.cient_id
        if batch.is_empty():
            self.clients[client_id][PENDING_EOF_POS] -= 1
            print(f"[GatewayOut] Pending EOF to receive: {self.clients[client_id][PENDING_EOF_POS]}")
            if not self.clients[client_id][PENDING_EOF_POS]:
                self.finished_client(client_id)
        else:
            batch.keep_fields()
            print(f"[GatewayOut] Sending result to client {client_id} with {batch.size()} elements")
            send_all(self.clients[client_id][CLIENT_SOCKET_POS], batch.to_bytes())
    
    def finished_client(self, client_id):
        print(f"[Gateway] No more EOF to receive. Sending EOF to client {client_id}")
        send_all(self.clients[client_id][CLIENT_SOCKET_POS], Batch([]))
        self.clients.pop(client_id)

    def close(self):
        for socket, _pending_eof in self.clients.values():
            socket.close()
        self.com.close_connection()

def gateway_out_main(recv_conn, eof_to_receive):
    signal_queue = Queue()
    com = Communicator.new(signal_queue)
    if not com:
        return
    gateway_out = GatewayOut(com, recv_conn, eof_to_receive)
    gateway_out.start()
