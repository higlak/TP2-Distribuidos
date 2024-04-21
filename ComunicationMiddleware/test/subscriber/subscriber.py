#!/usr/bin/env python3
import time
import sys

from middleware import Communicator

# Wait for rabbitmq to come up
time.sleep(10)

communicator = Communicator()
for i in range(10):
    message = communicator.receive_subscribed_message('prueba')
    print("", message.decode('utf-8'))
communicator.close_connection()