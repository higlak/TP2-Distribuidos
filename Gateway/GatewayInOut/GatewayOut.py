from queue import Queue
import signal

from CommunicationMiddleware.middleware import Communicator
from utils.Batch import Batch
from utils.auxiliar_functions import send_all

GATEWAY_QUEUE_NAME = 'Gateway'

class GatewayOut():
    def __init__(self, socket, com, eof_to_receive):
        self.socket = socket
        self.com = com
        self.eof_to_receive = eof_to_receive
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
            batch = Batch.from_bytes(batch_bytes)
            if batch.is_empty():
                self.eof_to_receive -= 1
                print(f"[GatewayOut] Pending EOF to receive: {self.eof_to_receive}")
                if not self.eof_to_receive:
                    print("[Gateway] No more EOF to receive. Sending EOF to client")
                    send_all(self.socket, batch.to_bytes())
                    break
            else:
                batch.keep_fields()
                print(f"[GatewayOut] Sending result to client with {batch.size()} elements")
                send_all(self.socket, batch.to_bytes())
    
    def close(self):
        self.socket.close()
        self.com.close_connection()

def gateway_out_main(client_socket, eof_to_receive):
    signal_queue = Queue()
    com = Communicator.new(signal_queue)
    if not com:
        return
    gateway_out = GatewayOut(client_socket, com, eof_to_receive)
    gateway_out.start()
