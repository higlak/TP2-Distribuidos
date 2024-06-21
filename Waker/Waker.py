from multiprocessing import Process
from queue import Empty, Queue
import signal
import socket
from time import sleep
import time
from utils.LeaderElection import ACK_MSG, BUFFER_BYTES, COORDINATOR_MSG, ELECTION_MSG
import heapq
import docker
from utils.Event import Event

WAKER_PORT = 5000

HEALTHCHECK_TIMEOUT = 10 # Timeout to hear a healthcheck from a leader
HEALTHCHECK_DELAY = 5 # Delay to send a healthcheck
ALIVE_TIMEOUT = 10 # Timeout to hear an alive message from a waker after sending healthcheck
ELECTION_TIMEOUT = 10 # Timeout to hear an ack or coord from a waker after sending election message

BUFFER_BYTES = 1

ELECTION_MSG = b'E'
ACK_MSG = b'A'
COORDINATOR_MSG = b'C'
HEALTHCHECK_MSG = b'H'
ALIVE_MSG = b'V'

ALIVE_TYPE = 'ALIVE'
HEALTHCHECK_TYPE = 'HEALTHCHECK'
    
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
        self.print(f'Listening on port : {WAKER_PORT}')

        try:
            self.start_leader_election()
        except OSError as e:
            self.print(f'Error with socket: {e}')

        self.print(f'Finished')

    def start_leader_election(self):
        self.print(f"Starting leader election")
        if self.have_biggest_id():
            self.print(f"I have the biggest id. I'm the new leader")
            self.handle_leader()
        else:
            msg, addr = self.send_election_messages()
            if not msg:
                self.print(f"I'm the new leader")
                self.handle_leader()
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
        
        self.socket.settimeout(ELECTION_TIMEOUT)
        msg, addr = self.receive_message() # Can receive either ACK or COORDINATOR

        return msg, addr
    
    def set_leader(self, leader_id):
        self.print(f"Setting {leader_id} as leader")
        self.leader_id = leader_id

    def handle_leader(self):
        self.set_leader(self.waker_id)
        self.set_events()
        self.broadcast_coordinator_message()

    def set_events(self):
        self.print(f"Setting event timeouts")
        self.events = []
        
        event = Event(HEALTHCHECK_TYPE, time.time() + HEALTHCHECK_DELAY)
        heapq.heappush(self.events, event)

        for container in self.wakers_containers:
            #if container < f'waker{self.waker_id}': # Por si soy lider pero hay un id mas grande que todavia no reconoci
            event = Event(ALIVE_TYPE, time.time() + ALIVE_TIMEOUT, container)
            heapq.heappush(self.events, event)

        for container in self.workers_containers:
            event = Event(ALIVE_TYPE, time.time() + ALIVE_TIMEOUT, container)
            heapq.heappush(self.events, event)
        
        self.show_events()
    
    def show_events(self):
        events = "Pending events:\n"
        for event in self.events:
            events += f'- {event}\n'
        self.print(f"{events[:-1]}")

    def broadcast_coordinator_message(self):
        self.print(f"Broadcasting coordinator messages")
        for waker_container in self.wakers_containers:
            self.send_message_to_container(COORDINATOR_MSG, waker_container)
    
    def send_message_to_container(self, message, waker_container):
        self.print(f"Sending {message.decode()} to: {waker_container}")
        try:
            self.socket.sendto(message, (waker_container, WAKER_PORT))
        except OSError:
            self.print(f"Error sending message to {waker_container}")

    def pending_healthcheck(self, event):
        return self.am_i_leader() and event.type == HEALTHCHECK_TYPE
    
    def pending_alive(self, event, addr):
        container_name = self.get_container_name_by_address(addr[0])
        return event.container_name != container_name and event.type == ALIVE_TYPE

    def loop(self):
        self.print(f"Starting main loop")
        while not self.finished:
            if self.am_i_leader():
                self.show_events()
                event = self.get_next_event()
            try:
                msg, addr = self.socket.recvfrom(BUFFER_BYTES)
                self.handle_message(msg, addr)
                if self.am_i_leader() and (self.pending_healthcheck(event) or self.pending_alive(event, addr)):  
                    heapq.heappush(self.events, event)

            except socket.timeout:
                self.print(f"Timeout waiting for message")
                if self.am_i_leader():
                    if event.type == HEALTHCHECK_TYPE:
                        self.broadcast_healthcheck()
                        event.increase_timeout(HEALTHCHECK_DELAY)
                    elif event.type == ALIVE_TYPE:
                        self.print(f"{event.container_name} didn't respond in time. Attempts remaining: {event.attemps}") 
                        if event.attemps == 0:
                            self.handle_container_reconnection(event.container_name)
                            event.restart_attemps()
                        else:
                            event.attemps -= 1
                        event.increase_timeout(ALIVE_TIMEOUT)
                    self.print(f"Event set: {event}")
                    heapq.heappush(self.events, event)
                    
                else:
                    self.start_leader_election()

    def get_next_event(self):
        event = heapq.heappop(self.events)
        self.print(f"Next event: {event}")
        new_timeout = max(event.timeout - time.time(), 0.1) # Si pongo 0 tira Resource Temporarily Unavailable
        self.socket.settimeout(new_timeout)
        self.print(f"Timeout set to {round(new_timeout, 2)}'s")
        return event

    def handle_message(self, msg, addr):
        self.print(f"Received {msg.decode()} from: {addr}")

        if msg == ACK_MSG:
            return
        
        if msg == HEALTHCHECK_MSG:
            self.send_message_to_container(ALIVE_MSG, addr[0])
            self.socket.sendto(ALIVE_MSG, addr)        

        if msg == ALIVE_MSG:
            self.update_container_timeout(self.get_container_name_by_address(addr[0]))

        if msg == COORDINATOR_MSG:
            waker_id = self.get_container_name_by_address(addr[0])
            if self.waker_id > waker_id:
                self.send_message_to_container(COORDINATOR_MSG, waker_id)
            else:
                self.set_leader(waker_id)
                self.socket.settimeout(HEALTHCHECK_TIMEOUT)
        
        if msg == ELECTION_MSG:
            self.socket.sendto(ACK_MSG, addr)
            self.start_leader_election()   

    def update_container_timeout(self, container_name):
        for event in self.events:
            if event.container_name == container_name:
                event.increase_timeout(ALIVE_TIMEOUT)
                self.print(f"Event set: {event}")
                return
        self.print(f"Container {container_name} alive timeout not found in events")
        new_event = Event(ALIVE_TYPE, time.time() + ALIVE_TIMEOUT, container_name)
        self.print(f"Event set: {new_event}")
        heapq.heappush(self.events, new_event)

    def broadcast_healthcheck(self):
        self.print(f"Broadcasting healthcheck messages")
        for waker_container in self.wakers_containers:
            #if waker_container < f'waker{self.waker_id}':
            self.send_message_to_container(HEALTHCHECK_MSG, waker_container)
        for worker_container in self.workers_containers:
            self.send_message_to_container(HEALTHCHECK_MSG, worker_container)

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
        if len(self.wakers_containers) == 0:
            return True
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
    
    def handle_container_reconnection(self, container_name):
        client = docker.from_env()
        self.print(f"Trying to revive {container_name}")
        try:
            container = client.containers.get(f'tp2-distribuidos-{container_name}-1')
            self.print(f"Found existing container for {container_name}, restarting it.")
            container.restart()
            self.print(f"Container {container_name} restarted.")
        except docker.errors.NotFound:
            self.print(f"No existing container found for {container_name}. Trying to create and start one.")
            try:
                container = client.containers.run(f'tp2-distribuidos-{container_name}', detach=True)
                self.print(f"Container created and started for {container_name}.")
            except docker.errors.ImageNotFound:
                self.print(f"No image found to create the container {container_name}.")
                return False
            except docker.errors.APIError as error:
                self.print(f"Failed to create or start the container {container_name}: {error}")
                return False
        return True
    
def extract_waker_name(container_name):
    parts = container_name.split('-')
    if len(parts) >= 2:
        return parts[-2]
    return None

