from queue import Empty, Queue
import signal
import socket
import time
import docker

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
        self.docker_client = docker.from_env()

    def handle_SIGTERM(self, _signum, _frame):
        print(f"[Waker {self.waker_id}] HearbeatReceiver for {self.container_name} SIGTERM detected")
        self.finished = True
        self.signal_queue.put(True)
        self.socket.close()

    def connect_to_container(self):
        i = STARTING_WAKER_WAIT
        while True:
            try:
                print(f"[Waker {self.waker_id}] Attempting connection to {self.container_name}")
                sockt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sockt.connect((self.container_name, HEARTBEAT_PORT))
                print(f"[Waker {self.waker_id}] Connected to {self.container_name}")
                return sockt
            except Exception as e:
                print(f"[Waker {self.waker_id}] Could not connect to {self.container_name}. {e}")
                if i > 2**MAX_ATTEMPTS:
                    print(f"[Waker {self.waker_id}] Could not connect to {self.container_name}. Max attempts reached")
                    return None
                print(f"[Waker {self.waker_id}] {self.container_name} not ready. Sleeping {i}s")
                try:
                    self.signal_queue.get(timeout=i)
                    print("[Communicator] SIGTERM received, exiting attempting connection")
                    return None
                except Empty:   
                    i *= 2

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

        self.socket = self.connect_to_container()
        if not socket:
            return 
        
        print(f"[Waker {self.waker_id}] Starting HeartbeatReceiver for {self.container_name}")
        self.socket.settimeout(WAKER_SOCKET_TIMEOUT)

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
                if not self.handle_container_reconnection():
                    break
                print(f"[Waker {self.waker_id}] Reconnected to {self.container_name}. Continuing...")
                continue
                
            print(f"[Waker {self.waker_id}] Received {recv_bytes.decode()} from {self.container_name}") # No se muestra

    def handle_container_reconnection(self):
        self.socket.close()
        print(f"[Waker {self.waker_id}] Trying to revive {self.container_name}")
        try:
            container = self.docker_client.containers.get(f'tp2-distribuidos-{self.container_name}-1')
            print(f"[Waker {self.waker_id}] Found existing container for {self.container_name}, restarting it.")
            container.restart()
            print(f"[Waker {self.waker_id}] Container {self.container_name} restarted.")
        except docker.errors.NotFound:
            print(f"[Waker {self.waker_id}] No existing container found for {self.container_name}. Trying to create and start one.")
            try:
                container = self.docker_client.containers.run(f'tp2-distribuidos-{self.container_name}', detach=True)
                print(f"[Waker {self.waker_id}] Container created and started for {self.container_name}.")
            except docker.errors.ImageNotFound:
                print(f"[Waker {self.waker_id}] No image found to create the container {self.container_name}.")
                return False
            except docker.errors.APIError as error:
                print(f"[Waker {self.waker_id}] Failed to create or start the container {self.container_name}: {error}")
                return False
            
        self.socket = self.connect_to_container()
        if not self.socket:
            return False
        return True