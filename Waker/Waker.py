import random
import signal
import socket
import time
import heapq
import docker
from utils.Event import Event

WAKER_PORT = 5000

# Delay to send a healthcheck
HEALTHCHECK_DELAY = 0.1

# Timeout to hear a healthcheck from a leader
HEALTHCHECK_TIMEOUT = HEALTHCHECK_DELAY * 6

# Timeout to hear an alive message from a waker after sending healthcheck
ALIVE_TIMEOUT = HEALTHCHECK_DELAY * 5

# Timeout to hear an ack or coord from a waker after sending election message
ACK_TIMEOUT = HEALTHCHECK_DELAY * 10

# Timeout to hear a coordinator message from a waker after sending election message
COORDINATOR_TIMEOUT = ACK_TIMEOUT * 1.5 

ELECTION_RETRIES = 6

# Type of messages
ELECTION_MSG = b'E'
ACK_MSG = b'A'
COORDINATOR_MSG = b'C'
HEALTHCHECK_MSG = b'H'
ALIVE_MSG = b'V'

BUFFER_BYTES = 1

NO_DELAY = 0.0000000000000000000000000000000000001

# UDP socket send retries for prevention
SEND_RETRIES = {
    ELECTION_MSG: 1,
    ACK_MSG: 1,
    COORDINATOR_MSG: 3,
    ALIVE_MSG: 1,
    HEALTHCHECK_MSG: 1
}

# Type of events
ALIVE_TYPE = 'ALIVE'
HEALTHCHECK_TYPE = 'HEALTHCHECK'
ACK_TYPE = "ACK"

class Waker():

    def __init__(self, waker_id ,workers_continers, wakers_containers):
        self.workers_containers = workers_continers
        self.wakers_containers = wakers_containers 
        self.waker_id = f'waker{waker_id}'
        self.leader_id = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.finished = False
        self.current_timeout = None
        self.events = []

        signal.signal(signal.SIGTERM, self.handle_SIGTERM)

    def handle_SIGTERM(self, _signum, _frame):
        self.print('SIGTERM detected\n\n')
        self.finished = True
        self.socket.close()

    def start(self):
        self.socket.bind(('', WAKER_PORT))
        self.print(f'Listening on port : {WAKER_PORT}')

        self.start_leader_election()
        self.loop()

        self.print(f'Finished')

    def set_timeout(self, timeout):
        self.socket.settimeout(timeout)
        self.current_timeout = timeout

    def start_leader_election(self):
        #self.print(f"Starting leader election")
        self.set_leader(None)

        if self.have_biggest_id():
            self.handle_leader()
        else:
            event = Event(ACK_TYPE, time.time() + ACK_TIMEOUT, attempts=ELECTION_RETRIES)
            self.send_election_messages(event)
        
    def send_election_messages(self, event):
        #self.print(f"Sending E messages to higher wakers")

        for waker_container in self.wakers_containers:
            if waker_container > self.waker_id:
                self.send_message_to_container(ELECTION_MSG, waker_container)
        
        heapq.heappush(self.events, event)
     
    def set_leader(self, leader_id):
        # if leader_id:
        #     self.print(f"Setting {leader_id} as leader")
        self.leader_id = leader_id

    def handle_leader(self):
        #self.print(f"I'm the leader")
        self.set_leader(self.waker_id)
        self.set_events()
        self.broadcast_coordinator_message()

    def set_events(self):
        #self.print(f"Setting event timeouts")
        self.events = []
        event = Event(HEALTHCHECK_TYPE, time.time() + HEALTHCHECK_DELAY)
        heapq.heappush(self.events, event)

        all_containers = self.wakers_containers + self.workers_containers
        random.shuffle(all_containers)

        for container in all_containers:
            event = Event(ALIVE_TYPE, time.time() + ALIVE_TIMEOUT, container)
            heapq.heappush(self.events, event)
        
        #self.show_events()
    
    def show_events(self):
        events = "Pending events:\n"
        for event in self.events:
            events += f'- {event}\n'
        self.print(f"{events[:-1]}")

    def broadcast_coordinator_message(self):
        #self.print(f"Broadcasting coordinator messages")
        for waker_container in self.wakers_containers:
            self.send_message_to_container(COORDINATOR_MSG, waker_container)
    
    def send_message_to_container(self, message, waker_container):
        #self.print(f"Sending {message.decode()} to: {waker_container}")

        send_retries = SEND_RETRIES[message]

        for _ in range(send_retries):
            try:
                self.socket.sendto(message, (waker_container, WAKER_PORT))
            except OSError as e:
                #self.print(f"Error sending message to {waker_container}: {e}")
                pass

    def pending_healthcheck(self, event):
        return event.type == HEALTHCHECK_TYPE
    
    def pending_alive(self, event, container_name):
        return event.container_name != container_name and event.type == ALIVE_TYPE

    def check_event_reinsertion(self, sender, msg, event):
        if event:
            if self.am_i_leader() and self.pending_event(event, sender):  
                heapq.heappush(self.events, event)
            if not self.am_i_leader() and event.type == ACK_TYPE and not (msg == ACK_MSG or msg == COORDINATOR_MSG):
                heapq.heappush(self.events, event)
        
    def loop(self):
        self.print(f"Starting main loop")
        while not self.finished:
            event = self.get_next_event()
            try:
                msg, addr = self.socket.recvfrom(BUFFER_BYTES)
                sender = self.get_container_name_by_address(addr)
                self.handle_message(msg, sender)
                self.check_event_reinsertion(sender, msg, event)
            except socket.timeout:
                if self.am_i_leader():
                    #self.print(f"Timeout waiting for message {event}")
                    self.handle_timeout_from_leader(event) 
                else:
                    self.handle_timeout_from_non_leader(event)
            except socket.error as e:
                self.print(f"Socket error: {e}")
                
    def handle_timeout_from_non_leader(self, event):
        if event:
            if event.type == ACK_TYPE:
                if event.attemps == 0:
                    self.handle_leader()
                else:
                    event.attemps -= 1
                    event.increase_timeout(ACK_TIMEOUT)
                    self.send_election_messages(event)
        else:
            self.start_leader_election()

    def pending_event(self, event, sender):
        return self.pending_healthcheck(event) or self.pending_alive(event, sender)

    def handle_timeout_from_leader(self, event):
        if event.type == HEALTHCHECK_TYPE:
            self.broadcast_healthcheck()
            event.increase_timeout(HEALTHCHECK_DELAY)
        elif event.type == ALIVE_TYPE:
            #self.print(f"{event.container_name} didn't respond in time. Attempts remaining: {event.attemps}") 
            if event.attemps == 0:
                self.handle_container_reconnection(event.container_name)
                event.restart_attemps()
            else:
                event.attemps -= 1
            event.increase_timeout(3* ALIVE_TIMEOUT)
        #self.print(f"Event set: {event}")
        heapq.heappush(self.events, event)

    def get_next_event(self):
        if len(self.events) == 0:
            return None
        heapq.heapify(self.events)
        #self.show_events()
        event = heapq.heappop(self.events)
        #self.print(f"Next event: {event}")
        new_timeout = max(event.timeout - time.time(), NO_DELAY)
        self.set_timeout(new_timeout)
        #self.print(f"Timeout set to {round(new_timeout, 2)}'s")
        return event

    def handle_message(self, msg, container_name):
        #self.print(f"Received {msg.decode()} from: {container_name}")

        if msg == ACK_MSG:
            self.set_timeout(COORDINATOR_TIMEOUT)
        
        if msg == HEALTHCHECK_MSG:
            self.send_message_to_container(ALIVE_MSG, container_name)      

        if msg == ALIVE_MSG:
            self.update_container_timeout(container_name)

        if msg == COORDINATOR_MSG:
            self.handle_coordinator_message(container_name)

        if msg == ELECTION_MSG:
            self.handle_election_message(container_name)

    def handle_coordinator_message(self, container_name):
        if self.waker_id > container_name:
            self.send_message_to_container(COORDINATOR_MSG, container_name)
        else:
            self.set_leader(container_name)
            self.events = []
            self.set_timeout(HEALTHCHECK_TIMEOUT)

    def handle_election_message(self, container_name):
        self.send_message_to_container(ACK_MSG, container_name)
        self.start_leader_election()   

    def update_container_timeout(self, container_name):
        for event in self.events:
            if event.container_name == container_name:
                event.increase_timeout(ALIVE_TIMEOUT)
                #self.print(f"Event updated: {event}")
                return
            
        #self.print(f"Container {container_name} alive timeout not found in events")
        new_event = Event(ALIVE_TYPE, time.time() + ALIVE_TIMEOUT, container_name)
        #self.print(f"Event set: {new_event}")
        heapq.heappush(self.events, new_event)

    def broadcast_healthcheck(self):
        #self.print(f"Broadcasting healthcheck messages")
        all_containers = self.wakers_containers + self.workers_containers
        for container in all_containers:
            self.send_message_to_container(HEALTHCHECK_MSG, container)

    def am_i_leader(self):
        return self.leader_id == self.waker_id
        
    def have_biggest_id(self):
        if len(self.wakers_containers) == 0:
            return True
        return max(self.wakers_containers) < self.waker_id
  
    def get_container_name_by_address(self, addr):
        container_name = socket.gethostbyaddr(addr[0])[0].strip('tp2-distribuidos-')
        return container_name.split('-')[0]

    def print(self, msg):
        waker_str = self.waker_id.capitalize()
        if self.am_i_leader():
            print(f"[{waker_str}] (Leader) {msg}", flush=True)
        else:
            print(f"[{waker_str}] {msg}", flush=True)
    
    def handle_container_reconnection(self, container_name):
        client = docker.from_env()
        #self.print(f"\n\nTrying to revive {container_name}\n")
        try:
            container = client.containers.get(f'tp2-distribuidos-{container_name}-1')
            #self.print(f"Found existing container for {container_name}, restarting it.")
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

