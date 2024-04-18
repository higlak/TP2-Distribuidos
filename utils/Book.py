from unittest import TestCase
from datetime import datetime, date

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

    def to_csv(self):
        return f'{self.title},{self.description},{self.authors},{self.image},{self.previewLink},{self.publisher},{self.publishedDate},{self.infoLink},{self.categories},{self.ratingsCount}'
        
    @classmethod
    def from_csv(self, attributes):
        title = attributes['Title']
        description = attributes['description'].strip('"')
        if not attributes['authors']:
            authors = []
        else:
            authors = attributes['authors'].strip('[').strip(']').split(',')
            authors = [author.strip(' ').strip('\'') for author in authors]
        image = attributes['image'] 
        previewLink = attributes['previewLink']
        publisher = attributes['publisher']
        if not attributes['publishedDate']:
            publishedDate = None
        else:
            publishedDate = datetime.strptime(attributes['publishedDate'], '%Y-%m-%d').date()
        infoLink = attributes['infoLink']
        if not attributes['categories']:
            categories = []
        else:
            categories = attributes['categories'].strip('[').strip(']').split(',')
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
        expected = Book('', '', [], '', '', '', None, '', [], None)
        self.assertEqual(vars(book), vars(expected))

    def test_full_book(self):
        attributes = {'Title': 'Murdocca', 'description': 'Libro de estructura del computador', 'authors': "['Autor1', 'Autor2']", 'image': 'imagen.png', 'previewLink': 'link.com', 'publisher': 'Mazzeo', 'publishedDate': '1840-04-19', 'infoLink': 'infolink.com', 'categories': "['Computacion']", 'ratingsCount': '4397.0'}
        book = Book.from_csv(attributes)
        expected = Book('Murdocca', 'Libro de estructura del computador', ['Autor1', 'Autor2'], 'imagen.png', 'link.com', 'Mazzeo', date(1840, 4, 19), 'infolink.com', ['Computacion'], 4397)
        self.assertEqual(vars(book), vars(expected))