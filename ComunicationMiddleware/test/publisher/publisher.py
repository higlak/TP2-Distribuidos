#!/usr/bin/env python3
import time

from middleware import Communicator

# Wait for rabbitmq to come up
time.sleep(10)

communicator = Communicator()
message = "Hola"
print(" [x] Sent %r" % message)
communicator.publish_message('prueba', message)
communicator.close_connection()
