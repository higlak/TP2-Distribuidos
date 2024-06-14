from multiprocessing import Process
from queue import Empty, Queue
import signal
import socket
from utils.HeartbeatReceiver import HeartbeatReceiver

HEARTBEAT_PORT = 1000
STARTING_WAKER_WAIT = 1
HEARTBEAT_DELAY = 1
MAX_ATTEMPTS = 5


class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = waker_id
        self.heartbeat_receiver_threads = []
        self.signal_queue = Queue()
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def start(self):
        print(f'[Waker {self.waker_id}] Starting')
        if not self.handle_containers(self.workers_containers):
            return
        for thread in self.heartbeat_receiver_threads:
            thread.start()
        for handle in self.heartbeat_receiver_threads:
            handle.join() 

    def handle_containers(self, containers):
        for container in containers:
            socket = self.connect_to_container(container)
            if not socket:
                return False
            heartbeat_receiver = HeartbeatReceiver(socket, container, self.waker_id)
            self.heartbeat_receiver_threads.append(Process(target=heartbeat_receiver.start))
        return True
        
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
                print(f"[Waker {self.waker_id}] {container_name} not ready")
                try:
                    self.signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2

    def handle_SIGTERM(self, _signum, _frame):
        print(f"[Waker {self.waker_id}] SIGTERM detected\n\n")
        self.signal_queue.put(True)
        for thread in self.heartbeat_receiver_threads:
            thread.terminate()
