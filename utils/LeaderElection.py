import socket

ELECTION_MSG = b'E'
ACK_MSG = b'A'
COORDINATOR_MSG = b'C'
ELECTION_PORT = 5000
BUFFER_BYTES = 1
INITIAL_MAX_ATTEMPS = 3

class LeaderElection():
    def __init__(self, waker_id, wakers_containers, waker_socket):
        self.waker_id = waker_id 
        self.wakers_containers = wakers_containers 
        self.leader_id = None 
        self.socket = waker_socket

    def start(self):
        n_attemps = 0
        ack = False
        while not ack and n_attemps < INITIAL_MAX_ATTEMPS:
            for waker_container in self.wakers_containers:
                if waker_container > f'waker{self.waker_id}':
                    self.send_message_to_container(ELECTION_MSG, waker_container)
            ack = self.receive_ack(n_attemps)
            n_attemps += 1

        if not ack and n_attemps == INITIAL_MAX_ATTEMPS:
            print(f"[Waker {self.waker_id}] I'm the new leader", flush=True)
            self.set_leader(self.waker_id)
            self.broadcast_coordinator_message()

    def broadcast_coordinator_message(self):
        print(f"[Waker {self.waker_id}] Broadcasting coordinator messages", flush=True)

    def send_message_to_container(self, message, waker_container):
        print(f"[Waker {self.waker_id}] Sending {message.decode()} to: {waker_container}", flush=True)
        self.socket.sendto(message, (waker_container, ELECTION_PORT))

    def send_message_to_addr(self, message, addr):
        print(f"[Waker {self.waker_id}] Sending {message.decode()} to: {addr}", flush=True)
        self.socket.sendto(message, addr)

    def am_i_leader(self):
        return self.leader_id == self.waker_id
    
    def set_leader(self, leader_id):
        self.leader_id = leader_id

    def receive_ack(self, n_attemps):
        print(f"[Waker {self.waker_id}] Waiting for ACK", flush=True)
        try:
            msg, addr = self.socket.recvfrom(BUFFER_BYTES)
            if msg == ACK_MSG:
                print(f"[Waker {self.waker_id}] Received ACK from: {addr}", flush=True)
                return True
        except socket.timeout:
            print(f"[Waker {self.waker_id}] Timeout waiting for ACK", flush=True)
            if n_attemps < INITIAL_MAX_ATTEMPS:
                print(f"[Waker {self.waker_id}] Retrying election. N attemp: {n_attemps}", flush=True)
            return False

