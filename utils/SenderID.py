from utils.auxiliar_functions import byte_array_to_big_endian_integer, integer_to_big_endian_byte_array, remove_bytes

ID_SEPARATOR = '.'
QUERY_BYTES = 2
POOL_BYTES = 2
SENDER_NUM_BYTES = 4
SENDER_ID_BYTES = QUERY_BYTES + POOL_BYTES + SENDER_NUM_BYTES 

import os

class SenderID():
    def __init__(self, query, pool_id, sender_num):
        self.query = query
        self.pool_id = pool_id
        self.sender_num = sender_num

    @classmethod
    def from_env(cls, env_var):
        env_id = os.getenv(env_var)
        if not env_id:
            print("Invalid worker id")
            return None
        query, pool_id, id = env_id.split(ID_SEPARATOR)
        try:
            query = int(query)
            pool_id = int(pool_id)
            id = int(id)
        except:
            return None
        return SenderID(query, pool_id, id)

    def to_bytes(self):
        byte_array = integer_to_big_endian_byte_array(self.query, QUERY_BYTES)
        byte_array.extend(integer_to_big_endian_byte_array(self.pool_id, POOL_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(self.sender_num, SENDER_NUM_BYTES))
        return byte_array

    @classmethod
    def from_bytes(cls, byte_array):
        if len(byte_array) < SENDER_ID_BYTES:
            return None
        query = byte_array_to_big_endian_integer(remove_bytes(byte_array,QUERY_BYTES))
        pool = byte_array_to_big_endian_integer(remove_bytes(byte_array,POOL_BYTES))
        sender_num = byte_array_to_big_endian_integer(remove_bytes(byte_array,SENDER_NUM_BYTES))
        return SenderID(query, pool, sender_num)

    #def get_worker_name(self):
    #    return f'{self.query}.{self.pool_id}.{self.sender_num}'
    @classmethod
    def from_string(cls, string):
        values = string.split('.')
        if len(values) != 3:
            return None
        query, pool_id, sender_num = values
        try:
            return cls(int(query), int(pool_id), int(sender_num))
        except:
            return None

    def __repr__(self):
        return f'{self.query}.{self.pool_id}.{self.sender_num}'
    
    def __eq__(self, other):
        if type(other) != SenderID:
            return False
        return self.query == other.query and self.pool_id == other.pool_id and self.sender_num == other.sender_num
    
    def __hash__(self):
        return hash((self.query, self.pool_id, self.sender_num))