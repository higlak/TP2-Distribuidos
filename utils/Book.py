import csv
from unittest import TestCase
from utils.Date import Date
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import unittest
import pprint
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.DatasetHandler import DatasetLine
import re

CSV_HEADER = "Title,description,authors,image,previewLink,publisher,publishedDate,infoLink,categories,ratingsCount"
BOOK_AS_CSV_LEN_BYTES = 2

class Book():
    def __init__(self, title='', description='', authors=[], image='', previewLink='', publisher='', publishedDate='', infoLink='', categories=[], ratingsCount=None):
        self.title = title
        self.description = description
        self.authors = authors
        self.image = image
        self.previewLink = previewLink
        self.publisher = publisher
        self.publishedYear = get_year_from_str(publishedDate)
        self.infoLink = infoLink
        self.categories = categories
        self.ratingsCount = ratingsCount

    def __eq__(self, other):
        return vars(self) == vars(other)
    
    def __repr__(self):
       return f'{self.title}\n{self.description}\n{self.authors}\n{self.image}\n{self.previewLink}\n{self.publisher}\n{self.publishedYear}\n{self.infoLink}\n{self.categories}\n{self.ratingsCount}'

    @classmethod
    def from_csv(cls, attributes):
        title = attributes['Title']
        description = attributes['description'].strip('"')
        if not attributes['authors']:
            authors = []
        else:
            authors = attributes['authors'].strip('"').strip('[').strip(']').strip(',').split(',')
            authors = [author.strip(' ').strip('\'') for author in authors]
            aux = []
            for author in authors:
                if author != '':
                    aux.append(author)
            authors = aux
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
    
    @classmethod
    def from_datasetline(cls, datasetLine: DatasetLine):
        dict_reader = csv.DictReader([CSV_HEADER, datasetLine.datasetLine])
        return cls.from_csv(next(dict_reader))

    def to_query1(self):
        if not self.publishedYear or not self.title or not self.authors or not self.publisher or not self.categories:
            return None
        return QueryMessage(BOOK_MSG_TYPE, year=self.publishedYear, title=self.title, authors=self.authors, publisher=self.publisher, categories=self.categories)

    def to_query2(self):
        if not self.publishedYear or not self.title or not self.authors:
            return None
        messages = []
        for author in self.authors:
            if author == "":
                print(self.authors)
            messages.append(QueryMessage(BOOK_MSG_TYPE, year=self.publishedYear, authors=[author]))
        return messages

    def to_query3(self):
        if not self.publishedYear or not self.title or not self.authors:
            return None
        return QueryMessage(BOOK_MSG_TYPE, year=self.publishedYear, rating=self.ratingsCount, title=self.title, authors=self.authors)
    
    def to_query5(self):
        if not self.title:
            return None
        return QueryMessage(BOOK_MSG_TYPE, title=self.title)

    def is_book(self):
        return True 
    
def get_year_from_str(string):
    coincidence = re.match('[^\d]*(\d{4})[^\d]*', string)
    if not coincidence:
        return None
    year = coincidence.group(1)
    return int(year)

class TestBook(TestCase):
    def expected_book(self):
        return Book('Murdocca', 'Libro de estructura del computador', ['Autor1', 'Autor2'], 'imagen.png', 'link.com', 'Mazzeo', "1840-04", 'infolink.com', ['Computacion'], 4397)
    
    def test_empty_book(self):
        attributes = {'Title': '', 'description': '', 'authors': '', 'image': '', 'previewLink': '', 'publisher': '', 'publishedDate': '', 'infoLink': '', 'categories': '', 'ratingsCount': ''}
        book = Book.from_csv(attributes)
        expected = Book('', '', [], '', '', '', '', '', [], None)
        self.assertEqual(book, expected)

    def test_full_book(self):
        attributes = {'Title': 'Murdocca', 'description': 'Libro de estructura del computador', 'authors': "\"['Autor1', 'Autor2']\"", 'image': 'imagen.png', 'previewLink': 'link.com', 'publisher': 'Mazzeo', 'publishedDate': '1840-04', 'infoLink': 'infolink.com', 'categories': "['Computacion']", 'ratingsCount': '4397.0'}
        book = Book.from_csv(attributes)
        expected = self.expected_book()
        self.assertEqual(book, expected)

    def test_from_datasetline(self):
        datasetline = DatasetLine("Murdocca,Libro de estructura del computador,\"['Autor1', 'Autor2']\",imagen.png,link.com,Mazzeo,1840-04,infolink.com,['Computacion'],4397.0", BOOK_MSG_TYPE)
        expected = self.expected_book()
        book = Book.from_datasetline(datasetline)
        self.assertEqual(book, expected)

    def test_strange_characters(self):
        datasetline = DatasetLine("Mensa Number Puzzles (Mensa Word Game<s for Kids),\"Acclaimed teacher and puzzler Evelyn B. Christensen has created one hundred brand-new perplexing and adorably illustrated games for young puzzlers. There is something for every type of learner here, including number puzzles, word puzzles, logic puzzles, and visual puzzles. She has also included secret clues the solver can consult if they need a hint, making the puzzles even more flexible for a wide skill range of puzzle-solvers. Arranged from easy to difficult, this is a great book for any beginning puzzler. With the game types intermixed throughout, it’s easy for a child who thinks they like only math or only word puzzles to stumble across a different kind of puzzle, get hooked, and discover—oh, they like that kind, too! Regularly practicing a variety of brain games can help improve and develop memory, concentration, creativity, reasoning, and problem-solving skills. Mensa’s® Fun Puzzle Challenges for Kids is a learning tool everyone will enjoy!\",['Evelyn B. Christensen'],http://books.google.com/books/content?id=tX1IswEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api,http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&cd=1&source=gbs_api,Sky Pony,2018-11-06,http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&source=gbs_api,['Juvenile Nonfiction'],", BOOK_MSG_TYPE)
        expected = Book("Mensa Number Puzzles (Mensa Word Game<s for Kids)", "Acclaimed teacher and puzzler Evelyn B. Christensen has created one hundred brand-new perplexing and adorably illustrated games for young puzzlers. There is something for every type of learner here, including number puzzles, word puzzles, logic puzzles, and visual puzzles. She has also included secret clues the solver can consult if they need a hint, making the puzzles even more flexible for a wide skill range of puzzle-solvers. Arranged from easy to difficult, this is a great book for any beginning puzzler. With the game types intermixed throughout, it’s easy for a child who thinks they like only math or only word puzzles to stumble across a different kind of puzzle, get hooked, and discover—oh, they like that kind, too! Regularly practicing a variety of brain games can help improve and develop memory, concentration, creativity, reasoning, and problem-solving skills. Mensa’s® Fun Puzzle Challenges for Kids is a learning tool everyone will enjoy!", ['Evelyn B. Christensen'], "http://books.google.com/books/content?id=tX1IswEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api","http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&cd=1&source=gbs_api", "Sky Pony","2018-11-06", "http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&source=gbs_api", ['Juvenile Nonfiction'])
        book = Book.from_datasetline(datasetline)
        self.assertEqual(book, expected)
if __name__ == '__main__':
    unittest.main()