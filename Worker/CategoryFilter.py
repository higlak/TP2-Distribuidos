from Worker import Worker
from utils.Message import Message, CATEGORIES_FIELD
import unittest
from unittest import TestCase

BOOK_MSG_TYPE = 0

class CategoryFilter(Worker):
    def __init__(self, category, fields_to_drop):
        self.category = category
        self.fields_to_drop = fields_to_drop

    def process_message(self, msg: Message):
        if msg.contains_category(self.category):
            return msg.drop(self.fields_to_drop)
        return None


class TestCategoryFilter(TestCase):
    
    def test_empty_message_to_bytes(self):
        msg = Message(BOOK_MSG_TYPE, 
                      year=1990,  
                      title='titulo', 
                      authors=['autor1', 'autor2'], 
                      categories=['Fantasia', 'Ciencia_ficcion'])
        worker = CategoryFilter('Fantasia', [CATEGORIES_FIELD])
        result = worker.process_message(msg)
        expected = Message(BOOK_MSG_TYPE, 
                           year=1990,  
                           title='titulo', 
                           authors=['autor1', 'autor2'])
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()