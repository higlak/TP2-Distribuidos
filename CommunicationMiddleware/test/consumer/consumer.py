#!/usr/bin/env python3
import os
import time
import sys

from middleware import Communicator

EXCHANGE_NAME = 'prueba'
N = 10

def main():
    
    id = os.getenv("CONSUMER_ID")

    communicator = Communicator()
    
    for _ in range(N):
        message, batch_id = communicator.consume_message(EXCHANGE_NAME) 
        print(message.decode('utf-8'))
    
    communicator.close_connection()

main()