#!/usr/bin/env python3
import os
import time
import sys

from middleware import Communicator

EXCHANGE_NAME = 'prueba'
N = 10

# Wait for rabbitmq to come up
time.sleep(10)

def main():
    
    id = os.getenv("CONSUMER_ID")
    communicator = Communicator()

    for _ in range(N):
        message = communicator.consume_message(EXCHANGE_NAME) 
        print(message.decode('utf-8'))
    
    communicator.close_connection()

main()