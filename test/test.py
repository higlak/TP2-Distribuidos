import docker
import time

def main():
    client = docker.from_env()

    containers = client.containers.run(
            "docker-compose",
            "up -d",  # Comando para levantar en modo detached
            remove=True,  # Eliminar los contenedores una vez terminada la ejecución
            detach=True,  # Ejecutar en modo detached
        )

    # Obtener el ID del contenedor que queremos monitorear
    containers_id = []
    for container in containers:
        if 'client' in container.name:
            containers_id.append(container.id)
            break

    for container_id in containers_id:
        status = client.containers.get(container_id).status
        while status ==  "running" or status == "starting" or status == "restarting":
            time.sleep(1)
            status = client.containers.get(container_id).status

    client.containers.prune()

main()