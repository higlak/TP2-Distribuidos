from unittest import TestCase
import unittest
import csv
from utils.DatasetHandler import DatasetLine
from utils.QueryMessage import QueryMessage, REVIEW_MSG_TYPE

CSV_HEADER = "Id,Title,Price,User_id,profileName,review/helpfulness,review/score,review/time,review/summary,review/text"

class Review():
    def __init__(self, id=None, title='', price='', user_id='', profileName='', helpfulness='', score='', time='', summary='', text=''):
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
        id = attributes['Id']
        title = attributes['Title'] 
        price = attributes['Price']
        user_id = attributes['User_id']    
        profileName = attributes['profileName']
        helpfulness = attributes['review/helpfulness']
        score = attributes['review/score']
        time = attributes['review/time']
        summary = attributes['review/summary']
        text = attributes['review/text']

        return Review(id, title, price, user_id, profileName, helpfulness, score, time, summary, text)

    @classmethod
    def from_datasetline(cls, datasetLine: DatasetLine):
        dict_reader = csv.DictReader([CSV_HEADER, datasetLine.datasetLine])
        return cls.from_csv(next(dict_reader))
    
    def to_query1(self):
        return None

    def to_query2(self):
        return None
    
    def to_query3(self):
        if not self.title:
            return None
        try:
            score = float(self.score)
        except:
            score = None
        return QueryMessage(REVIEW_MSG_TYPE, title=self.title, rating=score)
    
    def to_query5(self):
        if not self.title or not self.text:
            return None
        return QueryMessage(REVIEW_MSG_TYPE, title=self.title, review_text=self.text)

    def is_book(self):
        return False

class TestReview(TestCase):
    def expected_review(self):
        return Review('1', 'Murdocca', '10.0', '1', 'Mazzeo', '1/2', '4.0', '940636800', 'Libro de estructura del computador', 'Libro de estructura del computador')
        
    def test_empty_review(self):
        attributes = {'Id': '', 'Title': '', 'Price': '', 'User_id': '', 'profileName': '', 'review/helpfulness': '', 'review/score': '', 'review/time': '', 'review/summary': '', 'review/text': ''}
        review = Review.from_csv(attributes)
        expected = Review(None, '', '', '', '', '', '', '', '', '')
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
    unittest.main()