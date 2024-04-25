from configparser import ConfigParser
import os

CONFIG_FILE = "config/config_query"
FILTER_TYPE = 'Filter'
ACCUMULATOR_TYPE = 'Accumulator'

FILENAME = 'docker-compose-dev.yaml'
RABBIT = """  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672

"""
GATEWAY = """  gateway:
    build:
      context: ./
      dockerfile: Gateway/Gateway.dockerfile
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1"""

class Pool():
    def __init__(self, pool_number, config_pool):
        self.pool_number = pool_number
        self.worker_amount = int(config_pool["WORKER_AMOUNT"])
        self.worker_type = config_pool["WORKER_TYPE"]
        self.worker_field = config_pool["WORKER_FIELD"]
        self.worker_value = config_pool["WORKER_VALUE"]

    def worker_type_dokerfile_path(self):
        print(f"{self.worker_type},{FILTER_TYPE}")
        if self.worker_type == FILTER_TYPE:
            return "Filter/Filter.dockerfile"
        return "Accumulator/Accumulator.dockerfile"
        

class QueryConfig():
    def __init__(self, query_number, filename):
        config = ConfigParser()
        config.read(filename)
        self.query_number = query_number
        self.query_pools = []
        for i, pool_name in enumerate(config.sections()):
            self.query_pools.append(Pool(i, config[pool_name]))
    
    def to_docker_string(self):
        result = ""
        for p, pool in enumerate(self.query_pools):
            for i in range(pool.worker_amount):
                worker_id = f"{self.query_number}.{pool.pool_number}.{i}"
                result += f"""  worker{worker_id}:
    build:
      context: ./
      dockerfile: {pool.worker_type_dokerfile_path()}
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - WORKER_ID={worker_id}
      - NEXT_POOL_WORKERS={0 if p == len(self.query_pools) - 1 else self.query_pools[p].worker_amount}
      - WORKER_FIELD={pool.worker_field}
      - WORKER_VALUE={pool.worker_value}\n\n"""
                
        return result

def main(): 

    with open(FILENAME, "w") as file:
        file.write("version: '3'\nservices:\n")
        file.write(RABBIT)
        
        i=1 
        while True:
            filename = f'{CONFIG_FILE}{i}.ini'
            if not os.path.exists(filename):
                print("Last query processed: ", i-1)
                break
            q = QueryConfig(i, filename)
            file.write(q.to_docker_string())
            i+=1
        file.write(GATEWAY)
            

    print("hola")
        
        

main()