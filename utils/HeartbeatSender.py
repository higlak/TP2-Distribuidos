import signal
import socket
import time

from utils.auxiliar_functions import send_all

HEARTBEAT_DELAY = 5
HEARTBEAT_MSG = b'H'
HEARTBEAT_PORT = 1000
WAKER_SOCKET_TIMEOUT = 20

class HeartbeatSender():
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.finished = False

    def handle_heartbeat_SIGTERM(self, _signum, _frame):
        print(f"[Worker {self.worker_id}] HeartbeatSender SIGTERM detected\n\n")
        self.finished = True
        self.socket.close()

    def create_worker_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', HEARTBEAT_PORT))
        self.socket.settimeout(WAKER_SOCKET_TIMEOUT)
        self.socket.listen()
        print(f"[Worker {self.worker_id}] Listening for leader waker connection on port {HEARTBEAT_PORT}")    

    def accept_waker_leader(self):
        try:
            waker_socket, addr = self.socket.accept()
            print(f"[Worker {self.worker_id}] Accepted connection from waker leader {addr}")
            return waker_socket
        except OSError as e:
            print(f"[Worker {self.worker_id}] Error accepting waker leader connection. {e}")
        except socket.timeout:
            print(f"[Worker {self.worker_id}] Timeout waiting for leader waker connection")
            return None

    def start(self):
        print(f"[Worker {self.worker_id}] Starting HeartbeatSender")
        signal.signal(signal.SIGTERM, self.handle_heartbeat_SIGTERM)

        self.create_worker_socket()
        while True:           
            self.waker_socket = self.accept_waker_leader()

            if not self.waker_socket:
                return
            
            while not self.finished:
                if not self.send_heartbeat():
                    break
                time.sleep(HEARTBEAT_DELAY)

            print(f"[Worker {self.worker_id}] Closing waker leader connection")

    def send_heartbeat(self):
        print(f"[Worker {self.worker_id}] Sending heartbeat to waker leader")
        try:
            send_all(self.waker_socket, HEARTBEAT_MSG)
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error sending heartbeat to waker leader. {e}")
            self.waker_socket.close()
            return False
        return True