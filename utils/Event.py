import time

ALIVE_ATTEMPS = 3

class Event():
    def __init__(self, type, timeout, container_name=None):
        self.type = type
        self.container_name = container_name
        self.timeout = timeout
        self.attemps = ALIVE_ATTEMPS

    def did_timeout(self):
        return time.time() > self.timeout
    
    def restart_attemps(self):
        self.attemps = ALIVE_ATTEMPS

    def increase_timeout(self, ammount):
        self.timeout = time.time() + ammount

    def __str__(self):
        timeout = max(self.timeout - time.time(), 0)
        if self.container_name:
            return f"{self.type} TIMEOUT for {self.container_name} - {round(timeout, 2)}'s remaining"
        return f"NEXT {self.type} - {round(timeout, 2)}'s remaining" 
    
    def __lt__(self, other):
        return self.timeout < other.timeout