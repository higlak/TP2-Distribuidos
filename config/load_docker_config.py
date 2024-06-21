from configparser import ConfigParser
import os
import sys

QUERY_CONFIG_FILE = "config/config_query"
GATEWAY_CONFIG_FILE = "config/config_gateway.ini"
CLIENT_CONFIG_FILE = "config/config_client.ini"
WAKER_CONFIG_FILE = "config/config_waker.ini"
FILTER_TYPE = 'filter'
ACCUMULATOR_TYPE = 'accumulator'
REVIEW_TEXT_FIELD = 'review_text'
FORWARD_TO_SEPARATOR = ','
QUERY_POOL_SEPARATOR = '.'
GATEWAY = 'Gateway'
QUERIES = 5
DISTRIBUTE_BY_DEFAULT = ''

FILENAME = 'docker-compose-dev.yaml'
RABBIT = """  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672

"""

VOLUMES = """volumes:
  dataVolume:
    driver: local
    driver_opts:
      type: none
      device: ./data
      o: bind"""

class Pool():
    def __init__(self, pool_number, config_pool):
      self.pool_number = pool_number
      self.worker_amount = int(config_pool["WORKER_AMOUNT"])
      self.worker_type = config_pool["WORKER_TYPE"]
      self.worker_field = config_pool["WORKER_FIELD"]
      self.worker_value = config_pool["WORKER_VALUE"]
      self.forward_to = config_pool["FORWARD_TO"]
      try:
        self.distribute_by = config_pool["DISTRIBUTE_BY"]
      except:
        self.distribute_by = DISTRIBUTE_BY_DEFAULT
      self.accumulate_by = None
      if self.worker_type == ACCUMULATOR_TYPE:
        self.accumulate_by = config_pool["ACCUMULATE_BY"]

    def worker_type_dokerfile_path(self):
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
    
    def workers_last_pool(self):
      return self.query_pools[-1].worker_amount
    
    def worker_amount_of_pool(self, pool_num):
      return self.query_pools[pool_num].worker_amount
    
    def pool_distribute_by(self, pool_num):
      return self.query_pools[pool_num].distribute_by

    def to_docker_string(self, queries, eof_to_receive):
        result = ""
        workers_containers = []
        for p, pool in enumerate(self.query_pools):
            for i in range(pool.worker_amount):
                next_pool_workers, shard_by = get_next_pool_foward_info(queries, pool.forward_to) 
                worker_id = f"{self.query_number}.{pool.pool_number}.{i}"
                worker_container = f'{pool.worker_type}{worker_id}'
                workers_containers.append(worker_container) 
                result += f"""  {worker_container}:
    build:
      context: ./
      dockerfile: {pool.worker_type_dokerfile_path()}\n"""
                if pool.worker_type == ACCUMULATOR_TYPE and pool.worker_field == REVIEW_TEXT_FIELD:
                  result += f"      args:"         
                  result += f"\n        - TEXTBLOB=True\n"
                result += f"""    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - WORKER_ID={worker_id}
      - EOF_TO_RECEIVE={eof_to_receive.get(f"{self.query_number}.{pool.pool_number}", 1)}
      - NEXT_POOL_WORKERS={next_pool_workers}
      - FORWARD_TO={pool.forward_to}
      - SHARD_BY={shard_by}
      - WORKER_FIELD={pool.worker_field}
      - WORKER_VALUE={pool.worker_value}"""
                if pool.worker_type == ACCUMULATOR_TYPE:
                  result += f"\n      - ACCUMULATE_BY={pool.accumulate_by}"
                result += "\n\n"
                
        return result, workers_containers

def process_all_queries(file):
  queries = {}
  for i in range(1,QUERIES+1):
    filename = f'{QUERY_CONFIG_FILE}{i}.ini'
    query = process_query(file, filename, i)
    if query:
      queries[i] = query
  return queries
    
def process_query(file, query_filename, query_number):
  if not os.path.exists(query_filename):
      return None
  q = QueryConfig(query_number, query_filename)
  print("Processed query: ", query_number)
  return q

def process_gateway(queries, eof_to_receive, file):
  config = ConfigParser()
  try:
    config.read(GATEWAY_CONFIG_FILE)
  except:
    print("No valid flename for gateway")
    return None
  config = config["DEFAULT"]
  book_queries = config["BOOK_QUERIES"]
  review_queries = config["REVIEW_QUERIES"]
  forward_to, next_pool_workers, shard_by = get_forward_gateway(queries, book_queries, review_queries)
  gateway_str = f"""  gateway:
    build:
      context: ./
      dockerfile: Gateway/Gateway.dockerfile
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - PORT={config["PORT"]}
      - BOOK_QUERIES={config["BOOK_QUERIES"]}
      - REVIEW_QUERIES={config["REVIEW_QUERIES"]}
      - FORWARD_TO={forward_to}
      - SHARD_BY={shard_by}
      - NEXT_POOL_WORKERS={next_pool_workers}
      - EOF_TO_RECEIVE={eof_to_receive[GATEWAY]}\n\n"""
  file.write(gateway_str)
  print("Processed gateway")
  return config["PORT"]

def get_forward_gateway(queries, book_queries, review_queries):
  book_queries = book_queries.split(',')
  review_queries = review_queries.split(',')
  if review_queries[0] == "":
    review_queries = []
  query_names = set(book_queries + review_queries)
  forward_to = []
  for query_name in query_names:
    forward_to.append(f"{query_name}.0")
  forward_to = ",".join(forward_to)

  next_pool_workers, shard_by = get_next_pool_foward_info(queries, forward_to)
  return forward_to, next_pool_workers, shard_by
    
def get_next_pool_foward_info(queries, forward_to):
  next_pools = forward_to.split(FORWARD_TO_SEPARATOR)
  next_pool_workers = []
  shard_by = []
  for next_pool in next_pools:
    if next_pool == GATEWAY:
      next_pool_workers.append(str(1))
      shard_by.append(DISTRIBUTE_BY_DEFAULT)
    else:
      query_num, pool_num = next_pool.split(QUERY_POOL_SEPARATOR)
      query = queries[int(query_num)]
      worker_amount = query.worker_amount_of_pool(int(pool_num))
      next_pool_workers.append(str(worker_amount))
      pool_shard_by = query.pool_distribute_by(int(pool_num))
      shard_by.append(pool_shard_by)

  return ','.join(next_pool_workers), ','.join(shard_by)

def write_queries(file, queries, eof_to_receive):
   for query in queries.values():
    docker_string, containers_name = query.to_docker_string(queries, eof_to_receive)
    file.write(docker_string)
    return containers_name
    
def eof_to_receive(queries):
  eof_to_receive = {}
  for query in queries.values():
    for pool in query.query_pools:
      for next_to_send in pool.forward_to.split(','):
        eof_to_receive[next_to_send] = eof_to_receive.get(next_to_send, 0) + pool.worker_amount
  return eof_to_receive 

def process_waker(file, i, worker_containers, waker_containers):
  other_waker_containers = waker_containers[:i] + waker_containers[i+1:]
  waker_str = f"""  waker{i}:
    build:
      context: ./
      dockerfile: Waker/Waker.dockerfile
    environment:
      - WORKERS_CONTAINERS={";".join(worker_containers)}
      - WAKERS_CONTAINERS={";".join(other_waker_containers)}
      - WAKER_ID={i}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock\n\n"""
  file.write(waker_str)
  return True

def process_wakers(file, worker_containers):
  config = ConfigParser()
  try:
    config.read(WAKER_CONFIG_FILE)
  except:
    print("No valid flename for client")
    return False
  config = config["DEFAULT"]
  n_wakers = int(config['AMOUNT_OF_WAKERS'])
  
  worker_containers.append('gateway')

  wakers = [f"waker{i}" for i in range(n_wakers)]
  for i in range(n_wakers):
    if not process_waker(file, i, worker_containers, wakers):
      return False
  return True

def process_client(queries, port, file, config, i):
  client_str = f"""  client{i}:
    build:
      context: ./
      dockerfile: Client/Client.dockerfile
    restart: on-failure
    environment:
      - PYTHONUNBUFFERED=1
      - BATCH_SIZE={config["BATCH_SIZE"]}
      - QUERY_RESULTS_PATH={config["QUERY_RESULTS_PATH"]}
      - QUERIES={",".join([str(query.query_number) for query in queries.values()])}
      - SERVER_PORT={port}
    volumes:
      - dataVolume:/data\n\n"""
  file.write(client_str)
  return True
  
def process_clients(queries, port, file):
  config = ConfigParser()
  try:
    config.read(CLIENT_CONFIG_FILE)
  except:
    print("No valid flename for client")
    return False
  config = config["DEFAULT"]
  n_clients = int(config['AMOUNT_OF_CLIENTS'])
  for i in range(n_clients):
    if not process_client(queries, port, file, config, i):
      return False
  return True      
  
def main():

  with open(FILENAME, "w") as file:
    file.write("version: '3'\nservices:\n")
    file.write(RABBIT)

    if len(sys.argv) < 2:
      individual_query = 0
    else:
      individual_query = int(sys.argv[1])
    print(individual_query)

    if not individual_query:
      queries = process_all_queries(file)
    else:
      filename = f'{QUERY_CONFIG_FILE}{individual_query}.ini'
      queries = {individual_query :process_query(file, filename, individual_query)}

    eof = eof_to_receive(queries)
    worker_containers = write_queries(file, queries, eof)
    port = process_gateway(queries, eof, file)
    if not port:
      return
    if not process_clients(queries, port, file):
      return
    if not process_wakers(file, worker_containers):
      return      
    file.write(VOLUMES)

main()