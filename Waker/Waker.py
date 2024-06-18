from multiprocessing import Process
from queue import Empty, Queue
import signal
import socket
from time import sleep
from utils.LeaderElection import ACK_MSG, BUFFER_BYTES, COORDINATOR_MSG, ELECTION_MSG, LeaderElection
from utils.HeartbeatReceiver import HEARTBEAT_MSG, HeartbeatReceiver

LEADER_ELECTION_ACK_TIMEOUT = 5
CLEAN_MESSAGES_TIMEOUT = 5
WAKER_PORT = 5000

class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = waker_id
        self.heartbeat_receiver_threads = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.leader_election = LeaderElection(self.waker_id, self.wakers_containers, self.socket)
        
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def start(self):
        self.socket.bind(('', WAKER_PORT))
        self.socket.settimeout(LEADER_ELECTION_ACK_TIMEOUT)
        print(f'[Waker {self.waker_id}] Starting', flush=True)
        print(f'[Waker {self.waker_id}] Listening on port : {WAKER_PORT}', flush=True)

        if max(self.wakers_containers) < f'waker{self.waker_id}':
            self.handle_leader()
        else:
            self.leader_election.start()

        # if not self.handle_containers(self.workers_containers):
        #     return
        # for thread in self.heartbeat_receiver_threads:
        #     thread.start()
        # for handle in self.heartbeat_receiver_threads:
        #     handle.join() 

    def handle_containers(self, containers):
        for container in containers:
            heartbeat_receiver = HeartbeatReceiver(container, self.waker_id)
            self.heartbeat_receiver_threads.append(Process(target=heartbeat_receiver.start))
        return True

    def handle_SIGTERM(self, _signum, _frame):
        print(f"[Waker {self.waker_id}] SIGTERM detected\n\n", flush=True)
        for thread in self.heartbeat_receiver_threads:
            thread.terminate()

    def handle_leader(self):
        print(f"[Waker {self.waker_id}] I have the biggest id. I'm the new leader", flush=True)
        self.leader_election.set_leader(self.waker_id)
        self.clean_messages()

        # for waker_container in self.wakers_containers:
        #     self.leader_election.send_message(COORDINATOR_MSG, waker_container)
        # self.leader_election.receive_ack()

        # print(f"[Worker {self.waker_id}] Starting leader loop", flush=True)
        # while True:
        #     try:
        #         msg, addr = self.socket.recvfrom(BUFFER_BYTES)
        #         print(f"[Worker {self.waker_id}] Received {msg.decode()} from: {addr}", flush=True)
        #         if msg == HEARTBEAT_MSG:
        #             print(f"[Worker {self.waker_id}] Actualizo heap qsyo", flush=True)
        #         if msg == COORDINATOR_MSG:
        #             print(f"[Worker {self.waker_id}] Mando ACK y dejo de ser lider", flush=True)
        #         if msg == ELECTION_MSG:
        #             self.leader_election.send_message(ACK_MSG, addr)
        #     except socket.timeout:
        #         print(f"[Worker {self.waker_id}] Timeout waiting for ACK from: {addr}", flush=True)
        #         print(f"[Worker {self.waker_id}] Restarting {addr} container", flush=True)
        #         sleep(10)
            
    def clean_messages(self):
        self.socket.settimeout(CLEAN_MESSAGES_TIMEOUT)
        print(f"[Waker {self.waker_id}] Cleaning messages beforing sending COORDINATOR message", flush=True)
        while True:
            try:
                msg, addr = self.socket.recvfrom(BUFFER_BYTES)
                print(f"[Waker {self.waker_id}] Received {msg.decode()} from: {addr}", flush=True)
                if msg == ELECTION_MSG:
                    self.leader_election.send_message_to_addr(ACK_MSG, addr)
            except socket.timeout:
                break
        print(f"[Waker {self.waker_id}] Cleaned messages", flush=True)