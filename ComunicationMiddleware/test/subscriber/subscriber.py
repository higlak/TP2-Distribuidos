#!/usr/bin/env python3
import os
import time
import sys

from middleware import Communicator

EXCHANGE_NAME = 'prueba'
N = 10

# Wait for rabbitmq to come up
time.sleep(10)

# The subscriber receives messages with default routing key if its id is 1 or 2
# If its id is greater than 2,it receives messages with routing key = id
def main():
    
    id = os.getenv("SUBSCRIBER_ID")
    communicator = Communicator()

    for _ in range(N):
        if int(id) <= 2:
            message = communicator.receive_subscribed_message(EXCHANGE_NAME)
        else:
            message = communicator.receive_subscribed_message(EXCHANGE_NAME, id)    
        print(message.decode('utf-8'))
    
    communicator.close_connection()

main()