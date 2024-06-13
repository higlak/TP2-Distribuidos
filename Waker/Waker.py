from queue import Empty, Queue
import signal
import socket
from time import sleep
from utils.auxiliar_functions import send_all

HEARTBEAT_PORT = 1000
STARTING_WAKER_WAIT = 1
HEARTBEAT_STR = 'heartbeat'
HEARTBEAT_DELAY = 1
MAX_ATTEMPTS = 5

class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = waker_id
        self.signal_queue = Queue()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def start(self):
        self.monitor_containers()

    def monitor_containers(self):
        self.monitor_workers_containers()

    def monitor_workers_containers(self):
        for worker_container in self.workers_containers:
            worker_socket = self.connect_to_container(worker_container)
            self.monitor(worker_socket)

    def monitor(self, container_name):
        i = STARTING_WAKER_WAIT
        while True:
            try:
                print(f"[Waker {self.waker_id}] Attempting connection to {container_name}")
                sockt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sockt.connect((container_name, HEARTBEAT_PORT))
                print(f"[Waker {self.waker_id}] Connected to {container_name}")
                return sockt
            except:
                if i > 2**MAX_ATTEMPTS:
                    print(f"[Waker {self.waker_id}] Could not connect to {container_name}. Max attempts reached")
                    return None
                print(f"[Waker {self.waker_id}] {container_name} not ready")
                try:
                    self.signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2
    def receive_heartbeat(self, sockt):
        while True:
            data = sockt.recv(1024)

    def handle_SIGTERM(self, _signum, _frame):
        print(f"[Waker {self.waker_id}] SIGTERM detected\n\n")
        self.signal_queue.put(True)
