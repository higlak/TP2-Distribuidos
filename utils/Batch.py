from utils.auxiliar_functions import integer_to_big_endian_byte_array
from utils.QueryMessage import QueryMessage
from utils.SyncMessage import SyncMessage, SYNC_DONE_MSG_TYPE, SYNC_MSG_TYPE
import unittest
from unittest import TestCase

AMOUNT_OF_MESSAGES_BYTES = 1

class Batch():
    def __init__(self, messages):
        self.messages = messages
    
    @classmethod
    def from_bytes(cls, byte_array):
        amount_of_messages = byte_array[0]
        if amount_of_messages == 0:
            return Batch([])
        byte_array = byte_array[1:]
        messages = []
        for _ in range(amount_of_messages):
            if len(byte_array) == 0:
                break
            message = message_from_bytes()
            messages.append(message) 
        return Batch(messages)
    
    def to_bytes(self):
        byte_array = integer_to_big_endian_byte_array(len(self.messages), AMOUNT_OF_MESSAGES_BYTES)
        for message in self.messages:
            byte_array.extend(message.to_bytes())
        return byte_array
    
    def is_empty(self):
        return len(self.messages) == 0
    
    def size(self):
        return len(self.messages)
    
    def __iter__(self):
        return iter(self.messages)

    def __next__(self):
        return next(self.messages)

def message_from_bytes(byte_array):
        if byte_array[0] == SYNC_MSG_TYPE or byte_array[0] == SYNC_DONE_MSG_TYPE:
            return SyncMessage.from_bytes(byte_array)
        return QueryMessage.from_bytes(byte_array)
                 
class TestBatch(TestCase):
    def test_book_message1(self):
        return QueryMessage(BOOK_MSG_TYPE, 
                      year=1990, 
                      mean_sentiment_polarity=0.8, 
                      title='titulo', 
                      authors=['autor1', 'autor2'], 
                      review_text='review del texto')
    
    def test_book_message2(self):
        return QueryMessage(BOOK_MSG_TYPE, title='tiutlante')

    def test_expected_batch_bytes(self):
        byte_array = bytearray([2])
        byte_array.extend(self.test_book_message1().to_bytes())
        byte_array.extend(self.test_book_message2().to_bytes())
        return byte_array

    def test_empty_batch_to_bytes(self):
        batch = Batch([])
        self.assertEqual(batch.to_bytes(), bytearray([0]))

    def test_empty_batch_from_bytes(self):
        batch = Batch.from_bytes(bytearray([0]))
        self.assertEqual(batch.to_bytes(), bytearray([0]))
        
    def test_batch_to_bytes(self):
        batch_bytes = self.test_expected_batch_bytes()
        batch = Batch([self.test_book_message1(), self.test_book_message2()])
        self.assertEqual(batch.to_bytes(), batch_bytes)

    def test_batch_from_bytes(self):
        batch_bytes = self.test_expected_batch_bytes()
        batch = Batch.from_bytes(batch_bytes)
        self.assertEqual(batch.to_bytes(), batch_bytes)

if __name__ == '__main__':
    from utils.QueryMessage import BOOK_MSG_TYPE
    unittest.main()