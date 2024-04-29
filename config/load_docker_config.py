from configparser import ConfigParser
import os
import sys

QUERY_CONFIG_FILE = "config/config_query"
GATEWAY_CONFIG_FILE = "config/config_gateway.ini"
FILTER_TYPE = 'filter'
ACCUMULATOR_TYPE = 'accumulator'

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
      
    def to_docker_string(self):
        result = ""
        for p, pool in enumerate(self.query_pools):
            for i in range(pool.worker_amount):
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
      - NEXT_POOL_WORKERS={0 if p == len(self.query_pools) - 1 else self.query_pools[p].worker_amount}
      - WORKER_FIELD={pool.worker_field}
      - WORKER_VALUE={pool.worker_value}"""
                if pool.worker_type == ACCUMULATOR_TYPE:
                    result += f"\n      - ACCUMULATE_BY={pool.accumulate_by}"
                result += "\n\n"
                
        return result

def proccess_all_queries(file):
  i=1 
  queries = []
  while True:
      filename = f'{QUERY_CONFIG_FILE}{i}.ini'
      query = proccess_query(file,filename, i)
      if not query:
          break
      queries.append(query)
      i+=1
  return queries
    
def proccess_query(file, query_filename, query_number):
  if not os.path.exists(query_filename):
      return None
  q = QueryConfig(query_number, query_filename)
  file.write(q.to_docker_string())
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
      - TOTAL_LAST_WORKERS={total_last_workers}\n\n"""
  file.write(gateway_str)
  print("Processed gateway")


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
      queries = proccess_all_queries(file)
    else:
      filename = f'{QUERY_CONFIG_FILE}{individual_query}.ini'
      queries = [proccess_query(file, filename, individual_query)]

    process_gateway(queries, file)
    file.write(CLIENT)      

main()