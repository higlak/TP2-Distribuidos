import csv
from unittest import TestCase
from utils.Date import Date
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import unittest
import pprint

CSV_HEADER = "Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount"
BOOK_AS_CSV_LEN_BYTES = 2

class Book():
    def __init__(self, title, description, authors, image, previewLink, publisher, publishedDate, infoLink, categories, ratingsCount):
        self.title = title
        self.description = description
        self.authors = authors
        self.image = image
        self.previewLink = previewLink
        self.publisher = publisher
        self.publishedDate = publishedDate
        self.infoLink = infoLink
        self.categories = categories
        self.ratingsCount = ratingsCount

    def __eq__(self, other):
        return vars(self) == vars(other)
    
    def __repr__(self):
       return f'{self.title}\n{self.description}\n{self.authors}\n{self.image}\n{self.previewLink}\n{self.publisher}\n{self.publishedDate}\n{self.infoLink}\n{self.categories}\n{self.ratingsCount}'

    @classmethod
    def from_csv(cls, attributes):
        title = attributes['Title']
        description = attributes['description'].strip('"')
        if not attributes['authors']:
            authors = []
        else:
            authors = attributes['authors'].strip('"').strip('[').strip(']').split(',')
            authors = [author.strip(' ').strip('\'') for author in authors]
        image = attributes['image'] 
        previewLink = attributes['previewLink']
        publisher = attributes['publisher']
        publishedDate = attributes['publishedDate']
        infoLink = attributes['infoLink']
        if not attributes['categories']:
            categories = []
        else:
            categories = attributes['categories'].strip('"').strip('[').strip(']').split(',')
            categories = [category.strip(' ').strip('\'') for category in categories]
        if not attributes['ratingsCount']:
            ratingsCount = None
        else:
            ratingsCount = int(float(attributes['ratingsCount']))

        return Book(title, description, authors, image, previewLink, publisher, publishedDate, infoLink, categories, ratingsCount)
    
class TestBook(TestCase):
    def test_empty_book(self):
        attributes = {'Title': '', 'description': '', 'authors': '', 'image': '', 'previewLink': '', 'publisher': '', 'publishedDate': '', 'infoLink': '', 'categories': '', 'ratingsCount': ''}
        book = Book.from_csv(attributes)
        expected = Book('', '', [], '', '', '', '', '', [], None)
        self.assertEqual(book, expected)

    def test_full_book(self):
        attributes = {'Title': 'Murdocca', 'description': 'Libro de estructura del computador', 'authors': "\"['Autor1', 'Autor2']\"", 'image': 'imagen.png', 'previewLink': 'link.com', 'publisher': 'Mazzeo', 'publishedDate': '1840-04', 'infoLink': 'infolink.com', 'categories': "['Computacion']", 'ratingsCount': '4397.0'}
        book = Book.from_csv(attributes)
        expected = Book('Murdocca', 'Libro de estructura del computador', ['Autor1', 'Autor2'], 'imagen.png', 'link.com', 'Mazzeo', "1840-04", 'infolink.com', ['Computacion'], 4397)
        self.assertEqual(book, expected)

if __name__ == '__main__':
    unittest.main()