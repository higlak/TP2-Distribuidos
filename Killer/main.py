import os
import docker
import random
from time import sleep

def get_random_container():
    """
    Returns a random alive container
    """
    client = docker.from_env()
    while True:
        containers = client.containers.list() 
        container = random.choice(containers)
        if 'waker' in container.name and only_one_waker_alive(containers):
            return None
        if is_valid_container(container):
            return container

def is_valid_container(container):
    return container.status == 'running' and 'rabbit' not in container.name and 'killer' not in container.name

def only_one_waker_alive(containers):
    wakers = 0
    for container in containers:
        if 'waker' in container.name:
            wakers += 1
    if wakers == 1:
        return True
    return False

def main():
    print("[Killer] Starting killer")
    try:
        kill_delay_min = int(os.getenv('KILL_DELAY_MIN'))
        kill_delay_max = int(os.getenv('KILL_DELAY_MAX'))
        containers_to_kill_min = int(os.getenv('CONTAINERS_TO_KILL_MIN'))
        containers_to_kill_max = int(os.getenv('CONTAINERS_TO_KILL_MAX'))
    except:
        print("[Killer] Invalid env variables")
        return

    while True:
        containers_to_kill = random.randint(containers_to_kill_min, containers_to_kill_max)
        print(f"[Killer] Killing {containers_to_kill} containers", flush=True)
        for _ in range(containers_to_kill):
            try:
                random_container = get_random_container()
                if not random_container:
                    print("[Killer] Tried to kill a waker but only one is alive, skipping", flush=True)
                    continue
                print(f"[Killer] Killing {random_container.name}", flush=True)
                random_container.kill()
            except docker.errors.APIError as e:
                print(f"[Killer] Error killing container: {e}", flush=True)
                return
        delay = random.randint(kill_delay_min, kill_delay_max)
        print(f"[Killer] Sleeping for {delay} seconds", flush=True)
        sleep(delay)

main()