from .Worker import Worker
from utils.Message import Message, CATEGORIES_FIELD
import unittest
from unittest import TestCase

BOOK_MSG_TYPE = 0

class CategoryFilter(Worker):
    def __init__(self, category, fields_to_drop):
        super().__init__()
        self.category = category
        self.fields_to_drop = fields_to_drop

    def process_message(self, msg: Message):
        if msg.contains_category(self.category):
            print(f"fowarded {msg.title}")
            return msg.copy_droping_fields(self.fields_to_drop)
        print(f"dropped {msg.title}")
        return None