from configparser import ConfigParser
import os
import sys

QUERY_CONFIG_FILE = "config/config_query"
GATEWAY_CONFIG_FILE = "config/config_gateway.ini"
FILTER_TYPE = 'filter'
ACCUMULATOR_TYPE = 'accumulator'
FORWARD_TO_SEPARATOR = ','
QUERY_POOL_SEPARATOR = '.'
GATEWAY = 'Gateway'
QUERIES = 5

FILENAME = 'docker-compose-dev.yaml'
RABBIT = """  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672

"""
CLIENT = """  client:
    build:
      context: ./
      dockerfile: Client/Client.dockerfile
    restart: on-failure
    environment:
      - PYTHONUNBUFFERED=1"""

class Pool():
    def __init__(self, pool_number, config_pool):
      self.pool_number = pool_number
      self.worker_amount = int(config_pool["WORKER_AMOUNT"])
      self.worker_type = config_pool["WORKER_TYPE"]
      self.worker_field = config_pool["WORKER_FIELD"]
      self.worker_value = config_pool["WORKER_VALUE"]
      self.forward_to = config_pool["FORWARD_TO"]
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

    def to_docker_string(self, queries):
        result = ""
        for p, pool in enumerate(self.query_pools):
            for i in range(pool.worker_amount):
                next_pool_workers = get_next_pool_workers(queries, pool.forward_to) 
                worker_id = f"{self.query_number}.{pool.pool_number}.{i}"
                result += f"""  {pool.worker_type}{worker_id}:
    build:
      context: ./
      dockerfile: {pool.worker_type_dokerfile_path()}
    restart: on-failure
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - WORKER_ID={worker_id}
      - NEXT_POOL_WORKERS={next_pool_workers}
      - FORWARD_TO={pool.forward_to}
      - WORKER_FIELD={pool.worker_field}
      - WORKER_VALUE={pool.worker_value}"""
                if pool.worker_type == ACCUMULATOR_TYPE:
                    result += f"\n      - ACCUMULATE_BY={pool.accumulate_by}"
                result += "\n\n"
                
        return result

def process_all_queries(file):
  queries = [] 
  for i in range(QUERIES):
    filename = f'{QUERY_CONFIG_FILE}{i}.ini'
    query = process_query(file, filename, i)
    if query:
      queries.append(query)
  return queries
    
def process_query(file, query_filename, query_number):
  if not os.path.exists(query_filename):
      return None
  q = QueryConfig(query_number, query_filename)
  print("Processed query: ", query_number)
  return q

def process_gateway(queries, file):
  config = ConfigParser()
  total_last_workers = 0
  for query in queries:
     total_last_workers += query.workers_last_pool()
  try:
    config.read(GATEWAY_CONFIG_FILE)
  except:
    print("No valid flename")
    return False
  config = config["DEFAULT"]
  book_queries = config["BOOK_QUERIES"]
  review_queries = config["REVIEW_QUERIES"]
  forward_to, next_pool_workers = get_forward_gateway(queries, book_queries, review_queries)
  gateway_str = f"""  gateway:
    build:
      context: ./
      dockerfile: Gateway/Gateway.dockerfile
    restart: on-failure
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
      - NEXT_POOL_WORKERS={next_pool_workers}
      - TOTAL_LAST_WORKERS={total_last_workers}\n\n"""
  file.write(gateway_str)
  print("Processed gateway")

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

  return forward_to, get_next_pool_workers(queries, forward_to)
    
def get_next_pool_workers(queries, forward_to):
  next_pools = forward_to.split(FORWARD_TO_SEPARATOR)
  next_pool_workers = []
  for next_pool in next_pools:
    if next_pool == GATEWAY:
      next_pool_workers.append(str(1))
    else:
      print("Next pool: ", next_pool)
      query_num, pool_num = next_pool.split(QUERY_POOL_SEPARATOR)
      worker_amount = queries[int(query_num)-1].worker_amount_of_pool(int(pool_num))
      next_pool_workers.append(str(worker_amount))
  return ','.join(next_pool_workers)

def write_queries(file, queries):
   for query in queries:
    file.write(query.to_docker_string(queries))
    
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
      queries = [process_query(file, filename, individual_query)]

    write_queries(file, queries)

    process_gateway(queries, file)
    file.write(CLIENT)      

main()