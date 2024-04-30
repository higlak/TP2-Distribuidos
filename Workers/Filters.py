from .Worker import Worker
from utils.QueryMessage import QueryMessage, CATEGORIES_FIELD, YEAR_FIELD, TITLE_FIELD
import unittest
from unittest import TestCase

class Filter(Worker):
    def __init__(self, field, valid_values, droping_fields=[]):
        super().__init__()
        self.field = field
        self.valid_values = valid_values
        self.droping_fields = droping_fields

    def process_message(self, msg: QueryMessage):
        if self.filter_msg(msg):
            print(f"fowarded {msg.title}")
            msg = msg.copy_droping_fields(self.droping_fields)
            return self.transform_to_result(msg)
        print(f"dropped {msg.title}")
        return None
    
    def filter_msg(self, msg:QueryMessage):
        switch = {
            CATEGORIES_FIELD: msg.contains_category,
            YEAR_FIELD: msg.between_years,
            TITLE_FIELD: msg.contains_in_title,
        }
        method = switch.get(self.field, None)
        if not method:
            return False
        return method(self.valid_values)