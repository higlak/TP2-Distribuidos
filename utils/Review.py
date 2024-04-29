import datetime
from unittest import TestCase
import unittest
import csv
from utils.DatasetHandler import DatasetLine
from utils.QueryMessage import QueryMessage

CSV_HEADER = "Id,Title,Price,User_id,profileName,review/helpfulness,review/score,review/time,review/summary,review/text"

class Review():
    def __init__(self, id=None, title='', price=None, user_id=None, profileName='', helpfulness=None, score=None, time=None, summary='', text=''):
        self.id = id
        self.title = title
        self.price = price
        self.user_id = user_id
        self.profileName = profileName
        self.helpfulness = helpfulness
        self.score = score
        self.time = time
        self.summary = summary
        self.text = text

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __repr__(self) -> str:
        return f"{self.id}\n{self.title}\n{self.price}\n{self.user_id}\n{self.profileName}\n{self.helpfulness}\n{self.score}\n{self.time}\n{self.summary}\n{self.text}"

    @classmethod
    def from_csv(cls, attributes):
        if not attributes['Id']:
            id = None
        else:
            id = int(attributes['Id'])
        title = attributes['Title'] 
        if not attributes['Price']:
            price = None
        else:
            price = float(attributes['Price'])
        if not attributes['User_id']:
            user_id = None
        else:
            user_id = int(attributes['User_id'])    
        profileName = attributes['profileName']
        if not attributes['review/helpfulness']:
            helpfulness = None
        else: 
            aux = attributes['review/helpfulness'].split('/')
            helpfulness = (int(aux[0]), int(aux[1]))
        if not attributes['review/score']:
            score = None
        else:
            score = int(float(attributes['review/score']))
        if not attributes['review/time']:
            time = None
        else:
            time = int(attributes['review/time'])
        summary = attributes['review/summary']
        text = attributes['review/text'].strip('"')

        return Review(id, title, price, user_id, profileName, helpfulness, score, time, summary, text)

    @classmethod
    def from_datasetline(cls, datasetLine: DatasetLine):
        dict_reader = csv.DictReader([CSV_HEADER, datasetLine.datasetLine])
        return cls.from_csv(next(dict_reader))
    
    def to_query3(self):
        if not self.title:
            return None
        return QueryMessage(REVIEW_MSG_TYPE, title=self.title)
    
    def to_query5(self):
        if not self.title or not self.text:
            return None
        return QueryMessage(REVIEW_MSG_TYPE, title=self.title, review_text=self.text)

    def is_book(self):
        return False

class TestReview(TestCase):
    def expected_review(self):
        return Review(1, 'Murdocca', 10.0, 1, 'Mazzeo', (1, 2), 4, 940636800, 'Libro de estructura del computador', 'Libro de estructura del computador')
    
    def test_empty_review(self):
        attributes = {'Id': '', 'Title': '', 'Price': '', 'User_id': '', 'profileName': '', 'review/helpfulness': '', 'review/score': '', 'review/time': '', 'review/summary': '', 'review/text': ''}
        review = Review.from_csv(attributes)
        expected = Review(None, '', None, None, '', None, None, None, '', '')
        self.assertEqual(review, expected)

    def test_full_review(self):
        attributes = {'Id': '1', 'Title': 'Murdocca', 'Price': '10.0', 'User_id': '1', 'profileName': 'Mazzeo', 'review/helpfulness': '1/2', 'review/score': '4.0', 'review/time': '940636800', 'review/summary': 'Libro de estructura del computador', 'review/text': 'Libro de estructura del computador'}
        review = Review.from_csv(attributes)
        expected = self.expected_review()
        self.assertEqual(review, expected)
        
    def test_from_datasetline(self):
        datasetline = DatasetLine("1,Murdocca,10.0,1,Mazzeo,1/2,4.0,940636800,Libro de estructura del computador,Libro de estructura del computador", REVIEW_MSG_TYPE)
        expected = self.expected_review()
        review = Review.from_datasetline(datasetline)
        self.assertEqual(review, expected)

if __name__ == '__main__':
    from utils.QueryMessage import REVIEW_MSG_TYPE
    unittest.main()