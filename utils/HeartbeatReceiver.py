from queue import Empty, Queue
import signal
import socket
import time

from utils.auxiliar_functions import recv_exactly

HEARTBEAT_MSG = b'H'
HEARTBEAT_BYTES = 1
WAKER_SOCKET_TIMEOUT = 10
HEARTBEAT_PORT = 1000
STARTING_WAKER_WAIT = 1
HEARTBEAT_DELAY = 1
MAX_ATTEMPTS = 5

class HeartbeatReceiver():
    def __init__(self, container_name, waker_id):
        self.container_name = container_name
        self.waker_id = waker_id
        self.finished = False
        self.signal_queue = Queue()

    def handle_SIGTERM(self, _signum, _frame):
        print(f"\n\n [Waker {self.waker_id}] HearbeatReceiver for {self.container_name} SIGTERM detected\n\n")
        self.finished = True
        self.signal_queue.put(True)
        self.socket.close()

    def connect_to_container(self, container_name):
        i = STARTING_WAKER_WAIT
        while True:
            try:
                print(f"[Waker {self.waker_id}] Attempting connection to {container_name}")
                sockt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sockt.connect((container_name, HEARTBEAT_PORT))
                print(f"[Waker {self.waker_id}] Connected to {container_name}")
                return sockt
            except Exception as e:
                print(f"[Waker {self.waker_id}] Could not connect to {container_name}. {e}")
                if i > 2**MAX_ATTEMPTS:
                    print(f"[Waker {self.waker_id}] Could not connect to {container_name}. Max attempts reached")
                    return None
                print(f"[Waker {self.waker_id}] {container_name} not ready. Sleeping {i}s")
                try:
                    self.signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

        self.socket = self.connect_to_container(self.container_name)
        if not socket:
            return 
        
        print(f"[Waker {self.waker_id}] Starting HeartbeatReceiver for {self.container_name}")
        self.socket.settimeout(WAKER_SOCKET_TIMEOUT)
        aux = 0
        while not self.finished:
            try:
                print(f"[Waker {self.waker_id}] Waiting for heartbeat from {self.container_name}")
                recv_bytes = recv_exactly(self.socket, HEARTBEAT_BYTES)
            except socket.timeout:
                print(f"[Waker {self.waker_id}] Timeout for {self.container_name}")
                self.socket.close()
                break
            if not recv_bytes:
                print(f"[Waker {self.waker_id}] Connection to {self.container_name} lost")
                self.socket.close()
                break
            print(f"[Waker {self.waker_id}] Received {recv_bytes.decode()} from {self.container_name}")