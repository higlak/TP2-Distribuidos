import time

class Event():
    def __init__(self, type, timeout, container_name=None, attempts=0):
        self.type = type
        self.container_name = container_name
        self.timeout = timeout
        self.attemps = attempts
        self.starting_attemps = attempts

    def did_timeout(self):
        return time.time() > self.timeout
    
    def restart_attemps(self):
        self.attemps = self.starting_attemps

    def increase_timeout(self, ammount):
        self.timeout = time.time() + ammount

    def __str__(self):
        timeout = max(self.timeout - time.time(), 0)
        if self.container_name:
            return f"{self.type} TIMEOUT for {self.container_name} - {round(timeout, 2)}'s remaining"
        return f"NEXT {self.type} - {round(timeout, 2)}'s remaining" 
    
    def __lt__(self, other):
        return self.timeout < other.timeout