from Workers.Accumulators import Accumulator, REVIEW_COUNT
from utils.QueryMessage import ALL_MESSAGE_FIELDS, YEAR_FIELD
import os

def get_env_worker_field():
    worker_field = os.getenv('WORKER_FIELD')
    if not (worker_field in ALL_MESSAGE_FIELDS) and worker_field != REVIEW_COUNT:
        print("invalid Worker_FIELD env var: ", worker_field)
        return None
    return worker_field

def get_env_worker_value(worker_field):
    worker_value = os.getenv('WORKER_VALUE')
    if worker_field == YEAR_FIELD:
        worker_value = int(worker_value)
    return worker_value

def get_env_accumulate_by():
    accumulate_by = os.getenv('ACCUMULATE_BY')
    if not (accumulate_by in ALL_MESSAGE_FIELDS):
        print("invalid ACCUMULATE_BY env var: ", accumulate_by)
        return None
    return accumulate_by

def get_env_accumulator_vars():
    worker_field = get_env_worker_field()
    accumulate_by = get_env_accumulate_by()
    if (not worker_field) or (not accumulate_by):
        return (None, None, None)
    try:
        worker_value = get_env_worker_value(worker_field)
    except:
        print("Invalid format for comparison value")
        return (None, None, None)
    return worker_field, worker_value, accumulate_by

def main():
    worker_field, worker_value, accumulate_by = get_env_accumulator_vars() 
    print(f"Iniciando acumulador por {accumulate_by} => {worker_field} = {worker_value} ")
    if not worker_field:
        return
    worker = Accumulator.new(worker_field, worker_value, accumulate_by)
    if worker:
        worker.start()

main()