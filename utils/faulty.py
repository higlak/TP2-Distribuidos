import os
import random
import time

from Persistance.KeyValueStorage import KeyValueStorage
from Persistance.log import LogReadWriter
from Persistance.MetadataHandler import MetadataHandler
try:
    from Workers.Worker import Worker
    from Workers.Accumulators import Accumulator
    from Workers.Filters import Filter
except:
    from GatewayInOut.GatewayIn import GatewayIn
    from GatewayInOut.GatewayOut import GatewayOut

#PANIC_PROB = 0.0005
PANIC_PROB = 0.001

try:
    import docker
    client = docker.from_env()
except:
    pass

class FaultyError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

def set_worker_as_faulty_if_needed():
    set_classes_as_faulty_if_needed([Worker, Accumulator, Filter])

def set_classes_as_faulty_if_needed(classes):
    if os.getenv('FAULTY'):
        for c in classes:
            set_class_as_faulty(c)
        set_class_as_faulty(KeyValueStorage)
        set_class_as_faulty(LogReadWriter)

def burst(cls, method_name):
    #print("\n\n\n FAULTY \n\n\n")
    #container_id = os.environ.get('HOSTNAME')

    #container = client.containers.get(container_id)

    #container.kill()
    raise FaultyError(f"Clase: {cls}, metodo: {method_name}")

def get_new_method(cls, old_method, method_name, close):
    def new_method(self, *args, **kwargs):
        if random.random() < PANIC_PROB:
            burst(cls, method_name)
        return old_method(self, *args, **kwargs)
    return new_method

def set_class_as_faulty(cls, close=False):
    print(f"Setting {cls} as faulty")
    random.seed(int(time.time()) * 1234567898765)
    for method_name in dir(cls):
        if method_name.startswith('__'):
            continue
    
        method = getattr(cls, method_name)
        try:
            method.__self__
            continue
        except:
            if callable(method):
                setattr(cls, method_name, get_new_method(cls, method, method_name, close))
