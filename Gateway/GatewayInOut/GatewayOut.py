from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.SenderID import SenderID
from utils.auxiliar_functions import send_all

GATEWAY_QUEUE_NAME = 'Gateway'
GATEWAY_SENDER_ID = SenderID(0,0,1)
PENDING_EOF_POS = 1
CLIENT_SOCKET_POS = 0

class GatewayOut():
    def __init__(self, recv_conn, eof_to_receive):
        self.clients = {}
        self.com = None
        self.sigterm_queue = Queue()
        self.eof_to_receive = eof_to_receive
        self.recv_clients = recv_conn
        self.batches_awaiting_clients = {}
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [GatewayOut] SIGTERM detected\n\n")
        self.sigterm_queue.put(True)
        self.close()

    def start(self):
        try:
            self.loop()
        except OSError as e :
            print("[GatewayOut] Socket disconnected: {e}")
        self.close()

    def loop(self):
        while True:
            batch = self.get_batch()
            if not batch:
                print(f"[GatewayOut] Disconnected from MOM")
                break

            self.get_new_clients()
            self.proccess_batch(batch)

    def get_batch(self):
        batch_bytes = self.com.consume_message(GATEWAY_QUEUE_NAME)
        if batch_bytes == None:
            return None
        return Batch.from_bytes(batch_bytes)

    def get_new_clients(self):
        while self.recv_clients.poll():
            id, client_socket = self.recv_clients.recv()
            print(f"[GatewatOut] Received new client with id: {id}")
            self.clients[id] = [client_socket, self.eof_to_receive]
            for batch in self.batches_awaiting_clients.pop(id, []):
                self.proccess_batch(batch)
    
    def proccess_batch(self, batch):
        client_id = batch.client_id
        if not self.clients.get(client_id, None):
            self.batches_awaiting_clients[client_id] = self.batches_awaiting_clients.get(client_id, []) + [batch]
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
        send_all(self.clients[client_id][CLIENT_SOCKET_POS], Batch.eof(client_id, GATEWAY_SENDER_ID).to_bytes())
        self.clients.pop(client_id)

    def connect(self):
        self.com = Communicator.new(self.sigterm_queue)
        if not self.com:
            return False
        return True

    def close(self):
        for socket, _pending_eof in self.clients.values():
            socket.close()
        self.com.close_connection()

def gateway_out_main(recv_conn, eof_to_receive):
    gateway_out = GatewayOut(recv_conn, eof_to_receive)
    if gateway_out.connect():
        gateway_out.start()
