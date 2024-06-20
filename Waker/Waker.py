from multiprocessing import Process
from queue import Empty, Queue
import signal
import socket
from time import sleep
import time
from utils.LeaderElection import ACK_MSG, BUFFER_BYTES, COORDINATOR_MSG, ELECTION_MSG, LeaderElection
from utils.HeartbeatReceiver import HEARTBEAT_MSG
import heapq
import docker

WAKER_PORT = 5000
HEALTHCHECK_DELAY = 5
RECEIVE_DEFAULT_TIMEOUT = 10
ELECTION_PORT = 5000
ELECTION_MSG = b'E'
ACK_MSG = b'A'
COORDINATOR_MSG = b'C'
HEALTHCHECK_MSG = b'H'
ALIVE_MSG = b'V'
BUFFER_BYTES = 1

ALIVE_TYPE = 'alive'
HEALTHCHECK_TYPE = 'healthcheck'

class Event():
    def __init__(self, type, timeout, container_name=None):
        self.type = type
        self.container_name = container_name
        self.timeout = timeout

    def did_timeout(self):
        return time.time() > self.timeout
    
    def increase_timeout(self, ammount):
        self.timeout == time.time() + ammount

    def __str__(self):
        if self.container_name:
            return f"{self.type} for {self.container_name} at {self.timeout}"
        return f'send healthcheck at {self.timeout}' 
    
    def __lt__(self, other):
        return self.timeout < other.timeout
    
class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = waker_id
        self.leader_id = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.finished = False

        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        self.print('SIGTERM detected\n\n')
        self.finished = True
        self.socket.close()

    def start(self):
        self.socket.bind(('', WAKER_PORT))
        self.socket.settimeout(RECEIVE_DEFAULT_TIMEOUT)
        self.print('Starting')
        self.print(f'Listening on port : {WAKER_PORT}')
        self.print(f'Receive timeout set: {RECEIVE_DEFAULT_TIMEOUT}s')

        try:
            self.start_leader_election()
        except OSError:
            self.print(f'Error with socket')

        self.print(f'Finished')

    def start_leader_election(self):
        if self.have_biggest_id():
            self.print(f"I have the biggest id. I'm the new leader")
            self.handle_leader()
        else:
            msg, addr = self.send_election_messages()
            if not msg:
                self.print(f"I'm the new leader")
                self.leader_id = self.waker_id
                self.broadcast_coordinator_message()
            if msg == ACK_MSG:
                self.print(f"I'm not the leader. Waiting for COORDINATOR")
            if msg == COORDINATOR_MSG:
                waker_id = self.get_container_name_by_address(addr[0])
                self.set_leader(waker_id)
            
        self.loop()
    
    def send_election_messages(self):
        msg = None
        self.print(f"Sending E messages to higher wakers")
        for waker_container in self.wakers_containers:
            if waker_container > f'waker{self.waker_id}':
                self.send_message_to_container(ELECTION_MSG, waker_container)
        msg, addr = self.receive_message() # Can receive either ACK or COORDINATOR

        return msg, addr
    
    def set_leader(self, leader_id):
        self.print(f"Setting {leader_id} as leader")
        self.leader_id = leader_id

    def handle_leader(self):
        self.set_leader(self.waker_id)
        self.set_timeouts()
        self.broadcast_coordinator_message()

    def set_timeouts(self):
        # @TODO: hacer también con los workers
        self.print(f"Setting event timeouts")
        self.events = []
        for container in self.wakers_containers:
            event = Event(ALIVE_TYPE, time.time() + HEALTHCHECK_DELAY, container)
            heapq.heappush(self.events, event)
        event = Event(HEALTHCHECK_TYPE, time.time() + HEALTHCHECK_DELAY)
        heapq.heappush(self.events, event)
        self.show_events()
    
    def show_events(self):
        for event in self.events:
            self.print(f"Event set: {event}")

    def broadcast_coordinator_message(self):
        # @TODO: enviar a los workers también

        self.print(f"Broadcasting coordinator messages")
        for waker_container in self.wakers_containers:
            self.send_message_to_container(COORDINATOR_MSG, waker_container)
    
    def send_message_to_container(self, message, waker_container):
        self.print(f"Sending {message.decode()} to: {waker_container}")
        self.socket.sendto(message, (waker_container, ELECTION_PORT))

    def loop(self):
        self.print(f"Looping")
        while not self.finished:
            if self.am_i_leader():
                event = heapq.heappop(self.events)
                self.socket.settimeout(max(event.timeout - time.time(), 0))
            try:
                #self.socket.settimeout(CLEAN_MESSAGES_TIMEOUT) # Sacar del heap si soy lider y setear eso
                msg, addr = self.socket.recvfrom(BUFFER_BYTES)
                self.print(f"Received {msg.decode()} from: {addr}")
                
                if msg == HEALTHCHECK_MSG:
                    self.send_message_to_container(ALIVE_MSG, addr[0])
                    self.socket.sendto(ALIVE_MSG, addr)
                
                if msg == ACK_MSG:
                    continue 

                if msg == ALIVE_MSG:
                    self.update_container_timeout(self.get_container_name_by_address(addr[0]))

                if msg == COORDINATOR_MSG:
                    waker_id = self.get_container_name_by_address(addr[0])
                    if self.waker_id > waker_id:
                        self.send_message_to_container(COORDINATOR_MSG, waker_id)
                    else:
                        self.set_leader(waker_id)
                
                if msg == ELECTION_MSG:
                    self.socket.sendto(ACK_MSG, addr)
                    self.start_leader_election()

            except socket.timeout:
                self.print(f"Timeout waiting for message")
                if self.am_i_leader():
                    self.print(f"Flujo de eventos")
                    if event.type == HEALTHCHECK_TYPE:
                        self.broadcast_healthcheck()
                        event.increase_timeout(HEALTHCHECK_DELAY)
                    elif event.type == ALIVE_TYPE:
                        self.print(f"{event.container_name} didn't respond in time")
                        # @TODO restart conainer
                    heapq.heappush(self.events, event)
                    
                else:
                    self.start_leader_election()

    def update_container_timeout(self, container_name):
        for event in self.events:
            if event.container_name == container_name:
                event.increase_timeout(HEALTHCHECK_DELAY)
                self.print(f"Updated timeout for {container_name}: {event.timeout}")
                return

    def broadcast_healthcheck(self):
        # @TODO enviar a los workers también
        for waker_container in self.wakers_containers:
            self.send_message_to_container(HEALTHCHECK_MSG, waker_container)

    def am_i_leader(self):
        return self.leader_id == self.waker_id
    
    def receive_message(self):
        self.print(f"Waiting for message")
        try:
            msg, addr = self.socket.recvfrom(BUFFER_BYTES)
            self.print(f"Received {msg.decode()} from: {addr}")
            return msg, addr
        except socket.timeout:
            self.print(f"Timeout waiting for message")
            return None, None
        
    def have_biggest_id(self):
        return max(self.wakers_containers) < f'waker{self.waker_id}'

    def get_container_name_by_address(self, addr):
        # TEMPORALMENTE USAMOS ESTO. POSIBLE SOLUCION ENVIAR EL WAKERID como parte del mensaje?
        client = docker.from_env()
        containers = client.containers.list() 
        for container in containers:
            container_details = client.api.inspect_container(container.id)
            for network_settings in container_details['NetworkSettings']['Networks'].values():
                if network_settings['IPAddress'] == addr:
                    return extract_waker_name(container.name)
        return None

    def print(self, msg):
        if self.am_i_leader():
            print(f"[Waker {self.waker_id}] (Leader) {msg}", flush=True)
        else:
            print(f"[Waker {self.waker_id}] {msg}", flush=True)
    
def extract_waker_name(container_name):
    parts = container_name.split('-')
    if len(parts) >= 2:
        return parts[-2]
    return None

