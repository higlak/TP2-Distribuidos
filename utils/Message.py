from Date import *
from big_endian_conversion import *
import struct

BOOK_MSG_TYPE = 0
REVIEW_MSG_TYPE = 1

SEPARATOR = ","
TITLE_LEN_BYTES = 2
AUTHORS_LEN_BYTES = 2
PUBLISHER_LEN_BYTES = 1
CATEGORY_LEN_BYTES = 2
YEAR_BYTES = 2
RATING_BYTES = 2
MSP_BYTES = 4

TITLE_POSITION = 0
AUTHOR_POSITION = TITLE_POSITION + TITLE_LEN_BYTES
PUBLISHER_POSITION = AUTHOR_POSITION + AUTHORS_LEN_BYTES
CATEGORY_POSITION = PUBLISHER_POSITION + PUBLISHER_LEN_BYTES
YEAR_POSITION = CATEGORY_POSITION + CATEGORY_LEN_BYTES
RATING_POSITION = YEAR_POSITION + YEAR_BYTES
MSP_POSITION = RATING_POSITION + RATING_BYTES
HEADER_LEN = MSP_POSITION + MSP_BYTES


class BookMessage():
    def __init__(self, title="", authors=[], publisher="", categories=[], year=None, rating=None, mean_sentiment_polarity= None):
        self.msg_type = BOOK_MSG_TYPE
        self.title= title
        self.authors = authors
        self.publisher = publisher
        self.categories = categories
        self.year = year
        self.rating = rating
        self.mean_sentiment_polarity = mean_sentiment_polarity

    @classmethod
    def from_bytes(self, bytes):
        title_len = byte_array_to_big_endian_integer(bytes[:AUTHOR_POSITION])
        authors_len = byte_array_to_big_endian_integer(bytes[AUTHOR_POSITION:PUBLISHER_POSITION])
        publisher_len = byte_array_to_big_endian_integer(bytes[PUBLISHER_POSITION:CATEGORY_POSITION])
        category_len = byte_array_to_big_endian_integer(bytes[CATEGORY_POSITION:YEAR_POSITION])
        year = byte_array_to_big_endian_integer(bytes[YEAR_POSITION:HEADER_LEN])
        rating = byte_array_to_big_endian_integer(bytes[RATING_POSITION:MSP_POSITION])
        msp = struct.unpack('f',bytes[MSP_POSITION:HEADER_LEN])[0]
        bytes:bytearray = bytes[HEADER_LEN:]
        title = bytes[:title_len].decode()
        bytes:bytearray = bytes[title_len:]
        authors = bytes[:authors_len].decode().split(SEPARATOR)
        bytes:bytearray = bytes[authors_len:]
        publisher = bytes[:publisher_len].decode()
        bytes:bytearray = bytes[publisher_len:]
        categories = bytes[:category_len].decode().split(SEPARATOR)
        return BookMessage(title, 
                                  authors, 
                                  publisher,
                                  categories,
                                  year,
                                  rating,
                                  msp) 
    
    def to_bytes(self):
        author_str = f"{SEPARATOR}".join(self.authors)
        categories_str = f"{SEPARATOR}".join(self.categories)
        bytes = bytearray()
        bytes.extend(integer_to_big_endian_byte_array(len(self.title),TITLE_LEN_BYTES))
        bytes.extend(integer_to_big_endian_byte_array(len(author_str),AUTHORS_LEN_BYTES))
        bytes.extend(integer_to_big_endian_byte_array(len(self.publisher),PUBLISHER_LEN_BYTES))
        bytes.extend(integer_to_big_endian_byte_array(len(self.categories),CATEGORY_LEN_BYTES))
        bytes.extend(integer_to_big_endian_byte_array(self.year, YEAR_BYTES))
        bytes.extend(integer_to_big_endian_byte_array(self.rating, RATING_BYTES))
        bytes.extend(struct.pack('f',self.mean_sentiment_polarity))
        bytes.extend(self.title)
        bytes.extend(author_str)
        bytes.extend(self.publisher)
        bytes.extend(categories_str)