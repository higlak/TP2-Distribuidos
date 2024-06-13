import os

CONTAINERS_SEPARATOR = ";"

def get_env_waker_vars(containers):
    containers_names = os.getenv(containers)
    if not containers_names:
        # TODO: Chequear también si se puede hacer un split
        return None
    return containers_names.split(CONTAINERS_SEPARATOR)

def main():
    workers_containers = get_env_waker_vars('WORKERS_CONTAINERS')
    wakers_containers = get_env_waker_vars('WAKERS_CONTAINERS') 
    print(f"Iniciando waker con {workers_containers} y {wakers_containers}")
    if not workers_containers or not wakers_containers:
        return
    # waker = Waker.new(workers_containers, wakers_containers)
    # if waker:
    #     waker.start()
    print("Proceso finalizado")

main()