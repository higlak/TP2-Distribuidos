#!/usr/bin/env python3
import random
import time

from middleware import Communicator

# Wait for rabbitmq to come up
time.sleep(15)

communicator = Communicator()
for i in range(10):
    #message = "{}".format(random.randint(1,11))
    message = f"{i}"
    communicator.publish_message('prueba', message)
    print(" [x] Sent %r" % message)
    time.sleep(1)
communicator.close_connection()
