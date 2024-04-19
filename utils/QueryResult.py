from unittest import TestCase
import unittest

class QueryResult():
    def __init__(self, title=None, authors=None, publisher=None):
        self.title = title
        self.authors = authors
        self.publisher = publisher
    
    def get_csv_values(self):
        l = []
        for value in vars(self).values():
            if value:
                l.append(value)
        return l

class TestQueryResult(TestCase):
    def test_empty_result(self):
        result = QueryResult()
        expected = []
        self.assertEqual(result.get_csv_values(), expected)
    
    def test_non_empty_result(self):
        result = QueryResult("Murdoca", "['Mazzeo']")
        expected = ["Murdoca", "['Mazzeo']"]
        self.assertEqual(result.get_csv_values(), expected)

if __name__ == '__main__':
    unittest.main()