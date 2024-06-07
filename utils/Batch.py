from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, recv_exactly, remove_bytes
from utils.QueryMessage import QueryMessage, query_result_headers
import unittest
from unittest import TestCase

AMOUNT_OF_CLIENT_ID_BYTES = 4
AMOUNT_OF_MESSAGES_BYTES = 1
HEADER_LEN = AMOUNT_OF_CLIENT_ID_BYTES + AMOUNT_OF_MESSAGES_BYTES

class Batch():
    def __init__(self, client_id, messages):
        self.client_id = client_id
        self.messages = messages
    
    @classmethod
    def from_bytes(cls, byte_array):
        client_id, amount_of_messages = cls.get_header_fields_from_bytes(byte_array)
        if amount_of_messages == 0:
            return Batch.eof(client_id)
        byte_array = byte_array[HEADER_LEN:]
        messages = []
        for _ in range(amount_of_messages):
            if len(byte_array) == 0:
                break
            message = QueryMessage.from_bytes(byte_array)
            messages.append(message) 
        return Batch(client_id, messages)
    
    @classmethod
    def get_header_fields_from_bytes(cls, byte_array):
        if byte_array == None or len(byte_array) < HEADER_LEN:
            return None, None
        byte_array = bytearray(byte_array)
        client_id = byte_array_to_big_endian_integer(byte_array[:AMOUNT_OF_CLIENT_ID_BYTES])
        amount_of_messages = byte_array[AMOUNT_OF_CLIENT_ID_BYTES]
        return client_id, amount_of_messages

    @classmethod
    def from_socket(cls, sock, data_class=None):
        header_bytes = recv_exactly(sock, HEADER_LEN)
        client_id, amount_of_messages = cls.get_header_fields_from_bytes(header_bytes)
        if client_id == None or amount_of_messages == None:
            return None
        instances = []
        if data_class:
            for _ in range(amount_of_messages):
                instance = data_class.from_socket(sock)
                if not instance:
                    return None
                instances.append(instance)
        return Batch(client_id, instances)
    
    @classmethod
    def eof(cls, client_id):
        return Batch(client_id, [])
        
    def to_bytes(self):
        byte_array = integer_to_big_endian_byte_array(self.client_id, AMOUNT_OF_CLIENT_ID_BYTES)
        byte_array.extend(integer_to_big_endian_byte_array(len(self.messages), AMOUNT_OF_MESSAGES_BYTES))
        for i, message in enumerate(self.messages):
            byte_array.extend(message.to_bytes())
        return byte_array
    
    def get_hashed_batchs(self, attribute, amount_of_workers):
        hashed_messages = {}
        for msg in self:
            worker_to_send = msg.get_attribute_hash(attribute) % amount_of_workers
            hashed_messages[worker_to_send] = hashed_messages.get(worker_to_send, []) + [msg]
        
        return {w:Batch(self.client_id, messages) for w, messages in hashed_messages.items()}
    
    def keep_fields(self):
        new_messages = []
        for message in self.messages:
            new_messages.append(message.copy_keeping_fields(query_result_headers(message.msg_type)))
        self.messages = new_messages

    def is_empty(self):
        return len(self.messages) == 0
    
    def size(self):
        return len(self.messages)
    
    def __iter__(self):
        return iter(self.messages)

    def __next__(self):
        return next(self.messages)
    
    def __getitem__(self, index):
        return self.messages[index]
                 

if __name__ == '__main__':
    from utils.QueryMessage import BOOK_MSG_TYPE

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
            byte_array = integer_to_big_endian_byte_array(0, AMOUNT_OF_CLIENT_ID_BYTES)
            byte_array.append(2)
            byte_array.extend(self.test_book_message1().to_bytes())
            byte_array.extend(self.test_book_message2().to_bytes())
            return byte_array
        
        def test_expected_empty_batch_bytes(self):
            byte_array = integer_to_big_endian_byte_array(0, AMOUNT_OF_CLIENT_ID_BYTES)
            byte_array.append(0)
            return byte_array

        def test_empty_batch_to_bytes(self):
            batch = Batch(0, [])
            self.assertEqual(batch.to_bytes(), self.test_expected_empty_batch_bytes())

        def test_empty_batch_from_bytes(self):
            batch = Batch.from_bytes(self.test_expected_empty_batch_bytes())
            self.assertEqual(batch.to_bytes(), self.test_expected_empty_batch_bytes())
            
        def test_batch_to_bytes(self):
            batch_bytes = self.test_expected_batch_bytes()
            batch = Batch(0, [self.test_book_message1(), self.test_book_message2()])
            self.assertEqual(batch.to_bytes(), batch_bytes)

        def test_batch_from_bytes(self):
            batch = Batch.from_bytes(self.test_expected_batch_bytes())
            self.assertEqual(batch.to_bytes(), self.test_expected_batch_bytes())
    unittest.main()