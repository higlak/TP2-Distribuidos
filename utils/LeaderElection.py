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
        while True:
            msg, addr, n_attemps = self.send_election_messages()
            if not msg and n_attemps == INITIAL_MAX_ATTEMPS:
                print(f"[Waker {self.waker_id}] I'm the new leader", flush=True)
                self.set_leader(self.waker_id)
                self.broadcast_coordinator_message()
                break
            if msg == ACK_MSG:
                print(f"[Waker {self.waker_id}] I'm not the leader. Waiting for COORDINATOR", flush=True)
                msg, addr = self.receive_message()
                if not msg:
                    print(f"[Waker {self.waker_id}] Timeout waiting for COORDINATOR. Retrying election...", flush=True)
            if msg == COORDINATOR_MSG:
                self.set_leader(addr)
                break

    def send_election_messages(self):
        n_attemps = 0
        msg = None
        while not msg and n_attemps < INITIAL_MAX_ATTEMPS:
            print(f"[Waker {self.waker_id}] Sending ELECTION. N attemp: {n_attemps}", flush=True)
            for waker_container in self.wakers_containers:
                if waker_container > f'waker{self.waker_id}':
                    self.send_message_to_container(ELECTION_MSG, waker_container)
            msg, addr = self.receive_message() # Can receive either ACK or COORDINATOR
            n_attemps += 1

        return msg, addr, n_attemps
    
    def send_message_to_container(self, message, waker_container):
        print(f"[Waker {self.waker_id}] Sending {message.decode()} to: {waker_container}", flush=True)
        self.socket.sendto(message, (waker_container, ELECTION_PORT))

    def send_message_to_addr(self, message, addr):
        print(f"[Waker {self.waker_id}] Sending {message.decode()} to: {addr}", flush=True)
        self.socket.sendto(message, addr)


    def set_leader(self, leader_id):
        print(f"[Waker {self.waker_id}] Setting leader: {leader_id}", flush=True)
        self.leader_id = leader_id
    
    def leader(self):
        return self.leader_id

    def receive_message(self):
        print(f"[Waker {self.waker_id}] Waiting for message", flush=True)
        try:
            msg, addr = self.socket.recvfrom(BUFFER_BYTES)
            print(f"[Waker {self.waker_id}] Received {msg.decode()} from: {addr}", flush=True)
            return msg, addr
        except socket.timeout:
            print(f"[Waker {self.waker_id}] Timeout waiting for message", flush=True)
            return None, None

