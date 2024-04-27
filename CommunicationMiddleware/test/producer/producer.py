#!/usr/bin/env python3
import time

from middleware import Communicator

TEST_EXCHANGE = 'prueba'
SLEEP_TIME = 0.5
N = 10

# Wait for rabbitmq to come up
time.sleep(15)

def main():
    communicator = Communicator()
    for i in range(N):
        message1 = f"{i}"
        communicator.produce_message(TEST_EXCHANGE, message1)
        print("Sent: ", message1)
        time.sleep(SLEEP_TIME)
    communicator.close_connection()

main()

