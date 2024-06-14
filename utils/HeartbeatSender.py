import signal
import socket
import time

from utils.auxiliar_functions import send_all

HEARTBEAT_DELAY = 5
HEARTBEAT_MSG = b'H'
HEARTBEAT_PORT = 1000
WAKER_SOCKET_TIMEOUT = 10

class HeartbeatSender():
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.finished = False

    def handle_SIGTERM(self, _signum, _frame):
        print(f"\n\n [Worker {self.worker_id}] HeartbeatSender SIGTERM detected\n\n")
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
            print(f"[Worker {self.worker_id}] Accepted connection from waker leader")
            return waker_socket
        except socket.timeout:
            print(f"[Worker {self.worker_id}] Timeout waiting for leader waker connection")
            return None

    def start(self):
        print(f"[Worker {self.worker_id}] Starting HeartbeatSender")
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

        self.create_worker_socket()
        self.waker_socket = self.accept_waker_leader()
        
        while not self.finished:
            self.send_heartbeat()
            time.sleep(HEARTBEAT_DELAY)

    def send_heartbeat(self):
        print(f"[Worker {self.worker_id}] Sending heartbeat to waker leader")
        send_all(self.waker_socket, HEARTBEAT_MSG)
        