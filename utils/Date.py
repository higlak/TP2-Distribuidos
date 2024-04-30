import re
import unittest
from unittest import TestCase

class Date:
    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day    
    
    def __eq__(self, other):
        return vars(self) == vars(other)

    def __repr__(self):
        return f'{self.year}, {self.month}, {self.day}'

    @classmethod
    def from_str(cls, string):
        """
        Creates a date of format year-month-day
        """

        if len(string) == 0:
            return Date(None, None, None)
        date = string.split('-')
        date = list(map(int,date))
        for _ in range(3-len(date)):
            date.append(None)

        coincidencia = re.match('[^\d]*(\d{4})[^\d]*', string)
        date[2] = coincidencia.group(1)
        
        return Date(date[0], date[1], date[2])

class TestBook(TestCase):
    def test_empty_date(self):
        str = ''
        date = Date.from_str(str)
        expected = Date(None, None, None)
        self.assertEqual(vars(date), vars(expected))

    def test_only_year_date(self):
        str = '1940'
        date = Date.from_str(str)
        expected = Date(1940, None, None)
        self.assertEqual(vars(date), vars(expected))

    def test_year_and_month_date(self):
        str = '1940-04'
        date = Date.from_str(str)
        expected = Date(1940, 4, None)
        self.assertEqual(vars(date), vars(expected))

    def test_full_year_date(self):
        str = '1940-04-19'
        date = Date.from_str(str)
        expected = Date(1940, 4, 19)
        self.assertEqual(vars(date), vars(expected))

if __name__ == '__main__':
    unittest.main()