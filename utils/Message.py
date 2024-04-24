from utils.Date import Date
from utils.big_endian_conversion import *
import struct
import unittest
from unittest import TestCase

BOOK_MSG_TYPE = 0
REVIEW_MSG_TYPE = 1

SEPARATOR = ","
MSG_TYPE_BYTES = 1
TITLE_LEN_BYTES = 2
AUTHORS_LEN_BYTES = 2
PUBLISHER_LEN_BYTES = 1
CATEGORIES_LEN_BYTES = 2
REVIEW_TEXT_LEN_BYTES = 2
YEAR_BYTES = 2
RATING_BYTES = 2
MSP_BYTES = 4

MSG_TYPE_FIELD = 'msg_type' 
YEAR_FIELD = 'year'
RATING_FIELD = 'rating'
MSP_FIELD = 'mean_sentiment_polarity'
TITLE_FIELD = 'title'
AUTHOR_FIELD = 'authors'
PUBLISHER_FIELD = 'publisher'
CATEGORIES_FIELD = 'categories'
REVIEW_TEXT_FIELD = 'review_text'

class Message():
    def __init__(self, msg_type, year=None, rating=None, mean_sentiment_polarity= None, title=None, authors=None, publisher=None, categories=None, review_text=None):
        self.msg_type = msg_type
        self.year = year
        self.rating = rating
        self.mean_sentiment_polarity = mean_sentiment_polarity
        self.title= title
        self.authors = authors
        self.publisher = publisher
        self.categories = categories
        self.review_text = review_text
        
    @classmethod
    def from_bytes(self, byte_array):
        msg_type = remove_bytes(byte_array, 1)[0]
        generator = ParametersGenerator(byte_array)
        
        year = generator.next()
        rating = generator.next()
        msp = generator.next()
        title = generator.next()
        authors = generator.next()
        publisher = generator.next()
        categories = generator.next()
        review_text = generator.next()
        
        return Message(msg_type, year, rating, msp, title, authors, publisher, categories, review_text) 
    
    def fields_to_list(self):
        return [self.year, self.rating, self.mean_sentiment_polarity, self.title, self.authors, self.publisher, self.categories, self.review_text]
    
    def to_bytes(self):
        byte_array = bytearray([self.msg_type])
        
        byte_array.append(self.parameters_to_bytes())
        byte_array.extend(self.fixed_fields_to_bytes()) 
        byte_array.extend(self.variable_fields_to_bytes())
        byte_array.extend(self.variable_len_fields_values_to_bytes())
       
        return byte_array
    
    def parameters_to_bytes(self):
        parameters = 0b0
        mask = 0b1
        for parameter in self.fields_to_list():
            if parameter:
                parameters = parameters | mask
            mask = mask << 1
        return parameters

    def fixed_fields_to_bytes(self):
        byte_array = bytearray()
        byte_array.extend(integer_to_big_endian_byte_array(self.year, YEAR_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(self.rating, RATING_BYTES))
        if self.mean_sentiment_polarity:
            byte_array.extend(struct.pack('f',self.mean_sentiment_polarity))
        return byte_array
    
    def variable_fields_to_bytes(self):
        byte_array = bytearray()
        byte_array.extend(integer_to_big_endian_byte_array(length(self.title), TITLE_LEN_BYTES))

        if self.authors:
            authors_str = f"{SEPARATOR}".join(self.authors)
            byte_array.extend(integer_to_big_endian_byte_array(length(authors_str), AUTHORS_LEN_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(length(self.publisher), PUBLISHER_LEN_BYTES))
        if self.categories:
            categories_str = f"{SEPARATOR}".join(self.categories)
            byte_array.extend(integer_to_big_endian_byte_array(length(categories_str), CATEGORIES_LEN_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(length(self.review_text), REVIEW_TEXT_LEN_BYTES))
        return byte_array
    
    def variable_len_fields_values_to_bytes(self):
        byte_array = bytearray()
        if self.title:
            byte_array.extend(self.title.encode())
        if self.authors:
            authors_str = f"{SEPARATOR}".join(self.authors)
            byte_array.extend(authors_str.encode())
        if self.publisher:
            byte_array.extend(self.publisher.encode())
        if self.categories:
            categories_str = f"{SEPARATOR}".join(self.categories)
            byte_array.extend(categories_str.encode())
        if self.review_text:
            byte_array.extend(self.review_text.encode())  
        return byte_array
    
    def contains_category(self, category):
        if not self.categories:
            return False 
        return category in self.categories
    
    def copy_droping_fields(self, fields_to_drop):
        fields = vars(self)
        for field_to_drop in fields_to_drop:
            fields[field_to_drop] = None
        return Message(**fields)
    
    def __eq__(self, other):
        return self.fields_to_list() == other.fields_to_list()

class ParametersGenerator():
    def __init__(self, byte_array):
        self.parameters = remove_bytes(byte_array,1)[0]
        self.byte_array = byte_array
        self.interprete_later = []
        self.generator = self.loop()

    def next(self):
        return next(self.generator)

    def loop(self):
        mask = 0b1
        
        # handle fix length fields: year, rating and mean sentiment polarity 
        for _ in range(3):
            parameter = self.parameters & mask
            if parameter:
                yield self.interprete(parameter)
            else:
                yield None 
            mask = mask << 1

        # handle variable length fields: title, authors, publisher, categories and review text
        for _ in range(5):
            parameter = self.parameters & mask
            if parameter:
                self.interprete(parameter)
            else:
                self.interprete_later.append((None, None))
            mask = mask << 1
        
        # handle variable length fields values
        for (length, method) in self.interprete_later:
            if not length:
                yield None
            else:
                yield method(length)
    
    def interprete(self, parameter):
        switch = {
            0b1: self.interprete_year, 
            0b10: self.interprete_rating,
            0b100: self.interprete_mean_sentiment_polarity,
            0b1000: self.interprete_len_title,
            0b10000: self.interprete_len_authors,
            0b100000: self.interprete_len_publisher,
            0b1000000: self.interprete_len_categories,
            0b10000000: self.interprete_len_review_text,
        }
        method = switch.get(parameter, None)
        if not method:
            return None
        return method()

    def interprete_big_endian_integer(self, end):
        length = byte_array_to_big_endian_integer(self.remove_bytes(end))
        return length

    def interprete_year(self):
        return self.interprete_big_endian_integer(YEAR_BYTES)
    
    def interprete_rating(self):
        return self.interprete_big_endian_integer(RATING_BYTES)

    def interprete_mean_sentiment_polarity(self):
        msp = struct.unpack('f',self.remove_bytes(MSP_BYTES))[0]
        return msp
    
    def interprete_variable_field(self, len_bytes, later_method):
        length = self.interprete_big_endian_integer(len_bytes)
        self.interprete_later.append((length, later_method))
        return length
    
    def interprete_len_title(self):
        return self.interprete_variable_field(TITLE_LEN_BYTES, self.interprete_string)

    def interprete_len_authors(self):
        return self.interprete_variable_field(AUTHORS_LEN_BYTES, self.interprete_list)

    def interprete_len_publisher(self):
        return self.interprete_variable_field(PUBLISHER_LEN_BYTES, self.interprete_string)

    def interprete_len_categories(self):
        return self.interprete_variable_field(CATEGORIES_LEN_BYTES, self.interprete_list)

    def interprete_len_review_text(self):
        return self.interprete_variable_field(REVIEW_TEXT_LEN_BYTES, self.interprete_string)
       
    def interprete_string(self, length):
        text = self.remove_bytes(length).decode()        
        return text
    
    def interprete_list(self, length):
        list = self.remove_bytes(length).decode().split(SEPARATOR)  
        return list

    def remove_bytes(self, finish, start=0):
        return remove_bytes(self.byte_array, finish, start)

def length(iterable):
    """
    Receives and iterable and returns its length
    If fails, returns None
    """
    try:
        return len(iterable)
    except:
        return None
        
def remove_bytes(array, finish, start=0):
    elements = array[start:finish]
    del array[start:finish]
    return elements

class TestMessage(TestCase):
    def test_book_message_to_bytes(self):
        # Message = [year:1990, rating: None, msp: 0,8, titulo:'titulo', authors:['autor1, autor2'], publisher: None, categories: None, review_text:'review del texto']

        byte_array = bytearray([BOOK_MSG_TYPE])
        byte_array.append(0b10011101)
        byte_array.extend(integer_to_big_endian_byte_array(1990, 2))
        byte_array.extend(struct.pack('f',0.8))
        byte_array.extend(integer_to_big_endian_byte_array(len('titulo'), TITLE_LEN_BYTES))
        authors_text = 'autor1'+SEPARATOR+'autor2'
        byte_array.extend(integer_to_big_endian_byte_array(len(authors_text), AUTHORS_LEN_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(len('review del texto'), REVIEW_TEXT_LEN_BYTES))
        byte_array.extend('titulo'.encode())
        byte_array.extend(authors_text.encode())
        byte_array.extend('review del texto'.encode())
        return byte_array

    def test_empty_message_to_bytes(self):
        msg = Message(BOOK_MSG_TYPE)
        msg_bytes = msg.to_bytes()
        expected = bytearray([BOOK_MSG_TYPE, 0b0])
        self.assertEqual(msg_bytes, expected)

    def test_message_to_bytes(self):
        expected_bytes = self.test_book_message_to_bytes()
        msg = Message(BOOK_MSG_TYPE, 
                      year=1990, 
                      mean_sentiment_polarity=0.8, 
                      title='titulo', 
                      authors=['autor1', 'autor2'], 
                      review_text='review del texto')
        msg_bytes = msg.to_bytes()
        self.assertEqual(msg_bytes, expected_bytes)

    def test_message_from_bytes(self):
        msg_bytes = self.test_book_message_to_bytes()
        msg = Message.from_bytes(msg_bytes)
        self.assertEqual(msg.to_bytes(), self.test_book_message_to_bytes())
        self.assertEqual(len(msg_bytes), 0)

    def test_copy_dropping_fields(self):
        msg = Message(BOOK_MSG_TYPE, year=1990, title='titulo')
        expected_msg = Message(BOOK_MSG_TYPE, year=1990)
        self.assertEqual(msg.copy_droping_fields([TITLE_FIELD]), expected_msg)

if __name__ == '__main__':
    unittest.main()