import os
from time import sleep

from Waker import Waker

CONTAINERS_SEPARATOR = ";"

sleep(5) # TODO Esperar a que los workers se levanten para hacerles connect (ex backoff chequear)

def get_env_waker_vars(containers):
    containers_names = os.getenv(containers)
    if not containers_names:
        # TODO: Chequear también si se puede hacer un split
        return None
    return containers_names.split(CONTAINERS_SEPARATOR)

def main():
    workers_containers = get_env_waker_vars('WORKERS_CONTAINERS')
    wakers_containers = get_env_waker_vars('WAKERS_CONTAINERS') 
    waker_id = os.getenv("WAKER_ID")
    print(f"Iniciando waker con {workers_containers} y {wakers_containers}")
    if not workers_containers:
        return
    waker = Waker(waker_id, workers_containers, wakers_containers)
    waker.start()
    print("Proceso finalizado")

main()