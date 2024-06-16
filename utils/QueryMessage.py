import hashlib
from utils.auxiliar_functions import *
import struct
import unittest
from unittest import TestCase

BOOK_MSG_TYPE = 0
REVIEW_MSG_TYPE = 1
QUERY1_RESULT = 2
QUERY2_RESULT = 3
QUERY3_RESULT = 4
QUERY4_RESULT = 5
QUERY5_RESULT = 6

SEPARATOR = ","
MSG_TYPE_BYTES = 1
PARAMETERS_BYTES = 1
TITLE_LEN_BYTES = 2
AUTHORS_LEN_BYTES = 2
PUBLISHER_LEN_BYTES = 1
CATEGORIES_LEN_BYTES = 2
REVIEW_TEXT_LEN_BYTES = 2
YEAR_BYTES = 2
RATING_BYTES = 4
MSP_BYTES = 4
FIXED_PART_HEADER_LEN = MSG_TYPE_BYTES + PARAMETERS_BYTES

PARAMETER_LENS = [YEAR_BYTES, RATING_BYTES, MSP_BYTES, TITLE_LEN_BYTES, AUTHORS_LEN_BYTES, PUBLISHER_LEN_BYTES, CATEGORIES_LEN_BYTES, REVIEW_TEXT_LEN_BYTES]

MSG_TYPE_FIELD = 'msg_type' 
YEAR_FIELD = 'year'
RATING_FIELD = 'rating'
MSP_FIELD = 'mean_sentiment_polarity'
TITLE_FIELD = 'title'
AUTHOR_FIELD = 'authors'
PUBLISHER_FIELD = 'publisher'
CATEGORIES_FIELD = 'categories'
REVIEW_TEXT_FIELD = 'review_text'
ALL_MESSAGE_FIELDS = (MSG_TYPE_FIELD, YEAR_FIELD, RATING_FIELD, MSP_FIELD, TITLE_FIELD, AUTHOR_FIELD, PUBLISHER_FIELD, CATEGORIES_FIELD, REVIEW_TEXT_FIELD)

class QueryMessage():
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
        if len(byte_array) < 2:
            return None
        msg_type = remove_bytes(byte_array, 1)[0]
        try:
            generator = ParametersGenerator(byte_array)
            year = generator.next()
            rating = generator.next()
            msp = generator.next()
            title = generator.next()
            authors = generator.next()
            publisher = generator.next()
            categories = generator.next()
            review_text = generator.next()
        except:
            return None
        
        return QueryMessage(msg_type, year, rating, msp, title, authors, publisher, categories, review_text) 
    
    @classmethod
    def from_socket(cls, socket):
        fixed_part_header  = recv_exactly(socket, FIXED_PART_HEADER_LEN)
        if not fixed_part_header:
            return None
        msg_type = fixed_part_header[0]
        parameters = fixed_part_header[1]
        
        header_parameters_len = ParametersGenerator.determine_header_parameter_len(parameters)
        header_parameters = recv_exactly(socket, header_parameters_len)
        if not header_parameters:
            return None

        body_len = ParametersGenerator.determine_body_len(bytearray([parameters])+header_parameters)
        body = recv_exactly(socket, body_len)
        if not body:
            return None

        full_message_bytes = bytearray([msg_type, parameters]) + header_parameters + body
        return cls.from_bytes(full_message_bytes)

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
        if self.rating:
            byte_array.extend(struct.pack('f',self.rating))
        if self.mean_sentiment_polarity:
            byte_array.extend(struct.pack('f',self.mean_sentiment_polarity))
        return byte_array
    
    def variable_fields_to_bytes(self):
        byte_array = bytearray()
        byte_array.extend(integer_to_big_endian_byte_array(length(encode(self.title)), TITLE_LEN_BYTES))
    
        if self.authors:
            authors_str = f"{SEPARATOR}".join(self.authors)
            byte_array.extend(integer_to_big_endian_byte_array(length(encode(authors_str)), AUTHORS_LEN_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(length(encode(self.publisher)), PUBLISHER_LEN_BYTES))
        if self.categories:
            categories_str = f"{SEPARATOR}".join(self.categories)
            byte_array.extend(integer_to_big_endian_byte_array(length(encode(categories_str)), CATEGORIES_LEN_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(length(encode(self.review_text)), REVIEW_TEXT_LEN_BYTES))
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
    
    def get_attribute_to_hash(self, attribute):
        return str(getattr(self, attribute))

    def contains_category(self, category):
        if not self.categories:
            return False 
        return category in self.categories
    
    def between_years(self, year_range):
        #tiene que ser == none, si tuvieras el 0 te lo tomaria como none si no
        if self.year == None:
            return False
        return self.year >= year_range[0] and self.year <= year_range[1]

    def contains_in_title(self, word):
        return word.lower() in self.title.lower()
    
    def decade(self):
        if self.year == None:
            return None
        return (self.year // 10) * 10

    def copy_droping_fields(self, fields_to_drop):
        fields = vars(self)
        for field_to_drop in fields_to_drop:
            fields[field_to_drop] = None
        return QueryMessage(**fields)
    
    def copy_keeping_fields(self, fields_to_keep):
        fields = vars(self)
        for field in fields.keys():
            if field != MSG_TYPE_FIELD and not field in fields_to_keep:
                fields[field] = None
        return QueryMessage(**fields)

    def get_csv_values(self):
        csv_values = []
        for value in self.fields_to_list():
            if value:
                csv_values.append(value)
        return csv_values

    def __eq__(self, other):
        return self.fields_to_list() == other.fields_to_list()
    
def query_to_query_result(query):
    switch = {
        1: QUERY1_RESULT,
        2: QUERY2_RESULT,
        3: QUERY3_RESULT,
        4: QUERY4_RESULT,
        5: QUERY5_RESULT,
    }
    return switch[query]

def query_result_headers(query_result):
    switch = {
            QUERY1_RESULT: ['title','authors','publisher'],
            QUERY2_RESULT: ['authors'],
            QUERY3_RESULT: ['title','authors'],
            QUERY4_RESULT: ['title'],
            QUERY5_RESULT: ['title'],
        }
    return switch[query_result]    

class ParametersGenerator():
    def __init__(self, byte_array, only_header=False):
        self.parameters = remove_bytes(byte_array,1)[0]
        self.byte_array = byte_array
        self.interprete_later = []
        self.generator = self.loop()
        self.only_header = only_header
    
    @classmethod
    def determine_header_parameter_len(cls, parameters):
        mask = 0x1
        parameters_len = 0
        for i in range(8):
            parameter = parameters & mask
            if parameter:
                parameters_len += PARAMETER_LENS[i]
            mask = mask << 1
        return parameters_len
    
    @classmethod
    def determine_body_len(cls, byte_array):
        gen = cls(byte_array, True)
        body_len = 0
        for i in range(8):
            field_value = gen.next()
            if i >= 3:
                if not (field_value == None):
                    body_len += field_value
        return body_len
        

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
                l = self.interprete(parameter)
                if self.only_header:
                    yield l
            else:  
                self.interprete_later.append((None, None))
                if self.only_header:
                    yield None
            mask = mask << 1
        
        if not self.only_header:
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
        return struct.unpack('f',self.remove_bytes(RATING_BYTES))[0]

    def interprete_mean_sentiment_polarity(self):
        return struct.unpack('f',self.remove_bytes(MSP_BYTES))[0]
    
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
        text = self.remove_bytes(length)
        text_decoded = text.decode()    
        return text_decoded
    
    def interprete_list(self, length):
        list = self.remove_bytes(length).decode().split(SEPARATOR)  
        return list

    def remove_bytes(self, finish, start=0):
        return remove_bytes(self.byte_array, finish, start)

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
        msg = QueryMessage(BOOK_MSG_TYPE)
        msg_bytes = msg.to_bytes()
        expected = bytearray([BOOK_MSG_TYPE, 0b0])
        self.assertEqual(msg_bytes, expected)

    def test_message_to_bytes(self):
        expected_bytes = self.test_book_message_to_bytes()
        msg = QueryMessage(BOOK_MSG_TYPE, 
                      year=1990, 
                      mean_sentiment_polarity=0.8, 
                      title='titulo', 
                      authors=['autor1', 'autor2'], 
                      review_text='review del texto')
        msg_bytes = msg.to_bytes()
        self.assertEqual(msg_bytes, expected_bytes)

    def test_message_from_bytes(self):
        msg_bytes = self.test_book_message_to_bytes()
        msg = QueryMessage.from_bytes(msg_bytes)
        self.assertEqual(msg.to_bytes(), self.test_book_message_to_bytes())
        self.assertEqual(len(msg_bytes), 0)

    def test_copy_dropping_fields(self):
        msg = QueryMessage(BOOK_MSG_TYPE, year=1990, title='titulo')
        expected_msg = QueryMessage(BOOK_MSG_TYPE, year=1990)
        self.assertEqual(msg.copy_droping_fields([TITLE_FIELD]), expected_msg)

if __name__ == '__main__':
    unittest.main()