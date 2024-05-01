#!/usr/bin/env python3
import time

from middleware import Communicator

TEST_EXCHANGE = 'prueba'
SLEEP_TIME = 0.5
N = 10

def main():
    print("antes del communicator")
    communicator = Communicator(consumer_queue_names=[TEST_EXCHANGE])
    print("dpues del communicator")
    for i in range(N):
        message1 = f"{i}"
        print("hola")
        communicator.produce_message(message1)
        print("Sent: ", message1)
        time.sleep(SLEEP_TIME)
    communicator.close_connection()

main()

