import os
import random
import time
from Workers.Worker import Worker
from Workers.Accumulators import Accumulator
from Workers.Filters import Filter
from Persistance.KeyValueStorage import KeyValueStorage
from Persistance.log import LogReadWriter

PANIC_PROB = 0.0005

class FaultyError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

def set_faulty_if_needed():
    if os.getenv('FAULTY'):
        set_class_as_faulty(Worker)
        set_class_as_faulty(Accumulator)
        set_class_as_faulty(Filter)
        #set_class_as_faulty(KeyValueStorage)
        set_class_as_faulty(LogReadWriter)

def get_new_method(cls, old_method, method_name):
    def new_method(self, *args, **kwargs):
        if random.random() < PANIC_PROB:
            raise FaultyError(f"Clase: {cls}, metodo: {method_name}")
        return old_method(self, *args, **kwargs)
    return new_method

def set_class_as_faulty(cls):
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
                setattr(cls, method_name, get_new_method(cls, method, method_name))