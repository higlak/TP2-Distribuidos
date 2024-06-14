from multiprocessing import Process
from queue import Empty, Queue
import signal
import socket
from utils.HeartbeatReceiver import HeartbeatReceiver

class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = waker_id
        self.heartbeat_receiver_threads = []
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
            heartbeat_receiver = HeartbeatReceiver(container, self.waker_id)
            self.heartbeat_receiver_threads.append(Process(target=heartbeat_receiver.start))
        return True

    def handle_SIGTERM(self, _signum, _frame):
        print(f"[Waker {self.waker_id}] SIGTERM detected\n\n")
        for thread in self.heartbeat_receiver_threads:
            thread.terminate()
