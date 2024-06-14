import time

from utils.auxiliar_functions import send_all

HEARTBEAT_DELAY = 2
HEARTBEAT_MSG = 'heartbeat'

class HeartbeatSender():
    def __init__(self, socket, worker_id):
        self.socket = socket
        self.worker_id = worker_id

    def start(self):
        print(f"[Worker {self.worker_id}] Starting heartbeat sender")
        while True:
            self.send_heartbeat()
            time.sleep(HEARTBEAT_DELAY)

    def send_heartbeat(self):
        print(f"[Worker {self.worker_id}] Sending heartbeat")
        send_all(self.socket, HEARTBEAT_MSG.encode())