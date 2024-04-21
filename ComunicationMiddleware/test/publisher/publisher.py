#!/usr/bin/env python3
import random
import time

from middleware import Communicator

# Wait for rabbitmq to come up
time.sleep(15)

communicator = Communicator()
for i in range(10):
    #message = "{}".format(random.randint(1,11))
    message1 = f"{i}"
    message2 = f"{i*2}"
    communicator.publish_message('prueba', message1, '1')
    communicator.publish_message('prueba', message2, '2')
    print(" [x] Sent %r" % message1)
    print(" [x] Sent %r" % message2)
    time.sleep(1)
communicator.close_connection()
