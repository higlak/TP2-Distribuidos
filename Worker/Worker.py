from CommunicationMiddleware.middleware import Communicator
from abc import ABC, abstractmethod
import os

ID_SEPARATOR = '.'
GATEWAY_EXCHANGE_NAME = 'GATEWAY_EXCHANGE'

class Worker_ID():
    def __init__(self, query, pool_id, routing_ke):
        self.query
        self.pool_id
        self.routing_key
    
    @classmethod
    def from_env(env_var):
        query, pool_id, id = os.getenv(env_var).split(ID_SEPARATOR)
        return Worker_ID(query, pool_id, id)

    def get_query(self):
        return self.query
    
    def get_exchange_name(self):
        return f'{self.query}.{self.pool_id}'
    
    def get_routing_key(self):
        return self.routing_key
    
    def next_exchange_name(self):
        return f'{self.query}.{int(self.pool_id)+1}'
    
class Worker(ABC):
    def __init__(self):
        self.id = Worker_ID.from_env('WORKER_ID')
        self.next_pool_workers = os.getenv('NEXT_POOL_WORKERS')

        routing_keys = []
        for i in range(int(self.next_pool_workers)):
            routing_keys.append(str(i))
        self.communicator = Communicator(routing_keys=routing_keys)

    @abstractmethod
    def process_message(self):
        pass
    
    def receive_message(self):
        exchange_name = self.id.get_exchange_name()
        routing_key = self.id.get_routing_key()
        return self.communicator.receive_subscribed_message(exchange_name, routing_key)
        
    def start(self):
        while True:
            print(f"[Worker {self.id}] Waiting for message...")
            message = self.receive_message()
            print(f"[Worker {self.id}] Received message: {self.message}")
            result = self.process_message(message)
            print(f"[Worker {self.id}] Message proccesed")
            self.send_message(result)
            print(f"[Worker {self.id}] Message sent to ")

    def send_message(self, message):
        if self.next_pool_workers == 0:
            self.communicator.publish_message(GATEWAY_EXCHANGE_NAME, message)
        else:
            exchange_name = self.id.next_exchange_name()
            self.communicator.publish_message_next_routing_key(exchange_name, message)

    def stop(self):
        pass


#[3[][][]]                              
 
"""
[book]
[review]
---q1---
[tile, author, publishDate, publisher]
[tile, author, publisher]
[tile, author, publisher]
---q2---
[author, year]
[author]
---q3---
[title, author, year, rating]
[title]
[title, author, rating]
[title, author]
---q4---
[title, rating]
[title]
---q5---
[title, categories]
[title, review_text]
[tile]
[title, mean_sentiment_polarity]
"""