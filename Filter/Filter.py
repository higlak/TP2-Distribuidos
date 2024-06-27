from Workers.Filters import Filter
from utils.QueryMessage import ALL_MESSAGE_FIELDS, YEAR_FIELD, TITLE_FIELD, AUTHOR_FIELD
import os

from utils.faulty import set_worker_as_faulty_if_needed

def get_env_worker_field():
    worker_field = os.getenv('WORKER_FIELD')
    if not (worker_field in ALL_MESSAGE_FIELDS):
        print("invalid FILTER_FIELD env var: ", worker_field)
        return None
    return worker_field

def get_env_worker_value(filter_type):
    filter_value = os.getenv('WORKER_VALUE')
    if filter_type == YEAR_FIELD:
        filter_value = tuple(map(int, filter_value.split(',')))
    return filter_value

def get_drop_fields_of_filter_type(filter_type):
    if filter_type == TITLE_FIELD or filter_type == AUTHOR_FIELD:
        return []
    return [filter_type]

def get_env_filter_vars():
    filter_type = get_env_worker_field()
    if not filter_type:
        return (None, None, None)
    try:
        filter_value = get_env_worker_value(filter_type)
    except:
        print("Invalid format for comparison value")
        return (None, None, None)
    drop_fields = get_drop_fields_of_filter_type(filter_type)
    return filter_type, filter_value, drop_fields

def main():
    set_worker_as_faulty_if_needed()
    filter_field, filter_value, drop_fields = get_env_filter_vars() 
    print(f"Iniciando filtro por {filter_field} = {filter_value}")
    if not filter_field:
        return
    worker = Filter.new(filter_field,filter_value, drop_fields)
    if worker:
        worker.start()
    print("Proceso finalizado")

main()