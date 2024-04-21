#!/usr/bin/env python3
import time
import sys

from middleware import Communicator

# Wait for rabbitmq to come up
time.sleep(10)

communicator = Communicator()
message = communicator.receive_subscribed_message('prueba')
print(message)
communicator.close_connection()