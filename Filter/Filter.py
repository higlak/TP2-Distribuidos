from time import sleep
from Workers.Filters import Filter
from utils.Message import ALL_MESSAGE_FIELDS, YEAR_FIELD, CATEGORIES_FIELD, TITLE_FIELD
import os

def get_env_filter_type():
    filter_type = os.getenv('FILTER_TYPE')
    if not (filter_type in ALL_MESSAGE_FIELDS):
        print("invalid FILTER_TYPE env var: ", filter_type)
        return None
    return filter_type

def get_env_filter_value(filter_type):
    filter_value = os.getenv('FILTER_VALUE')
    if filter_type == YEAR_FIELD:
        filter_value = tuple(map(int, filter_value.split(',')))
    return filter_value

def get_drop_fields_of_filter_type(filter_type):
    if filter_type == TITLE_FIELD:
        return []
    return [filter_type]

def get_env_filter_vars():
    filter_type = get_env_filter_type()
    if not filter_type:
        return (None, None, None)
    try:
        filter_value = get_env_filter_value(filter_type)
    except:
        print("Invalid format for comparison value")
        return (None, None, None)
    drop_fields = get_drop_fields_of_filter_type(filter_type)
    return filter_type, filter_value, drop_fields

sleep(15)
def main():
    filter_type, filter_value, drop_fields = get_env_filter_vars() 
    print(f"inciando con :{filter_type} {filter_value} {drop_fields}")
    if not filter_type:
        return
    worker = Filter(filter_type,filter_value, drop_fields)
    worker.start()
    print("hola")

main()