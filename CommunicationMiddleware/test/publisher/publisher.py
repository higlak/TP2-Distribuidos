#!/usr/bin/env python3
import time

from middleware import Communicator

TEST_MESSAGE = 'prueba'
ROUTING_KEY = '3'
SLEEP_TIME = 0.5
N = 10

# Wait for rabbitmq to come up
time.sleep(15)

# This publisher sends numbers from 0 to 9 to subscribers with default routing key
# and sends even numbers from 0 to 18 to subscribers with routing key = 3
def main():
    communicator = Communicator()
    for i in range(N):
        message1 = f"{i}"
        message2 = f"{i*2}"
        communicator.publish_message(TEST_MESSAGE, message1)
        communicator.publish_message(TEST_MESSAGE, message2, ROUTING_KEY)
        print("Sent: ", message1)
        print("Sent: ", message2)
        time.sleep(SLEEP_TIME)
    communicator.close_connection()

main()

