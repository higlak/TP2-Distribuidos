import time

from utils.auxiliar_functions import recv_exactly, recv_exactly_timeout

HEARTBEAT_MSG = 'heartbeat'
HEARTBEAT_BYTES = len(HEARTBEAT_MSG) 
WAKER_SOCKET_TIMEOUT = 6

class HeartbeatReceiver():
    def __init__(self, socket, container_name, waker_id):
        self.socket = socket
        self.container_name = container_name
        self.waker_id = waker_id

    def start(self):
        #last_heartbeat = time.time()
        print(f"[Waker {self.waker_id}] Starting healthbeat checker for {self.container_name}")
        while True:
            self.socket.settimeout(WAKER_SOCKET_TIMEOUT)
            recv_bytes = recv_exactly_timeout(self.socket, HEARTBEAT_BYTES)
            if not recv_bytes:
                print(f"[Waker {self.waker_id}] Connection to {self.container_name} lost")
                break
            print(f"[Waker {self.waker_id}] Received heartbeat from {self.container_name}")
