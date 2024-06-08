from abc import ABC, abstractmethod
import struct
from Persistance.storage_errors import *
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import io

#lista de u32
#u32 float
#float 
#u32 float
#float

MAX_LEN_LIST = 10
LEN_LIST_BYTES = 1
STARTING_FILE_POS = io.SEEK_SET
FIXED_STR_LEN = 128
U32_BYTES = 4
FIXED_FLOAT_BYTES = 4
STR_PADDING = bytes([0xff])

class StorableTypes(ABC):
    @abstractmethod
    def to_bytes(self):
        pass

    @classmethod
    @abstractmethod
    def from_bytes(cls, byte_array):
        pass

    def from_types(t):
        switch = {
            str: FixedStr,
            int: U32,
            (list, int): FixedU32List,
            float: FixedFloat
        }
        storable_t = switch.get(t, None)
        if storable_t == None:
            raise UnsupportedType(f"{t.__name__} not supported")
        return switch[t]
    
    @classmethod
    @abstractmethod
    def size(cls):
        pass

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.value)

class FixedStr(StorableTypes):
    def __init__(self, string):
        if not isinstance(string, str):
            raise TypeDoesNotMatchSetType
        self.value = string
    
    def to_bytes(self):
        return bytearray(self.value.encode()).ljust(FIXED_STR_LEN, STR_PADDING)
    
    @classmethod
    def from_bytes(self, byte_array):
        return FixedStr(byte_array.strip(STR_PADDING).decode())

    @classmethod
    def size(cls):
        return FIXED_STR_LEN

class U32(StorableTypes):
    def __init__(self, num):
        if not isinstance(num, int):
            raise TypeDoesNotMatchSetType
        self.value = num

    def to_bytes(self):
        return integer_to_big_endian_byte_array(self.value, U32_BYTES)

    @classmethod
    def from_bytes(self, byte_array):
        return U32(byte_array_to_big_endian_integer(byte_array))

    @classmethod
    def size(cls):
        return U32_BYTES
    
class FixedU32List(StorableTypes):
    def __init__(self, numbers):
        if not isinstance(numbers, list):
            raise TypeDoesNotMatchSetType
        for num in numbers:
            if not isinstance(num, int):
                raise TypeDoesNotMatchSetType
        self.value = numbers[:MAX_LEN_LIST]

    def to_bytes(self):
        byte_array = bytearray(integer_to_big_endian_byte_array(len(self.value), LEN_LIST_BYTES))
        numbers = self.value + [0] * (MAX_LEN_LIST - len(self.value))
        for num in numbers:
            byte_array.extend(integer_to_big_endian_byte_array(num, U32_BYTES))
        return byte_array

    @classmethod
    def from_bytes(self, byte_array):
        numbers = []
        len_numbers = byte_array_to_big_endian_integer(remove_bytes(byte_array, LEN_LIST_BYTES))
        while len(byte_array) > U32_BYTES:
            if len(numbers) < len_numbers:
                numbers.append(byte_array_to_big_endian_integer(remove_bytes(byte_array,U32_BYTES)))
            else:
                break
        return FixedU32List(numbers)

    @classmethod
    def size(cls):
        return MAX_LEN_LIST * U32_BYTES
    
class FixedFloat(StorableTypes):
    def __init__(self, number):
        if not isinstance(number, float):
            raise TypeDoesNotMatchSetType
        self.value = number

    def to_bytes(self):
        return bytearray(struct.pack('f',self.value))

    @classmethod
    def from_bytes(self, byte_array):
        return FixedFloat(struct.unpack('f', byte_array)[0])

    @classmethod
    def size(cls):
        return FIXED_FLOAT_BYTES

class KeyValueStorage():
    def __init__(self, file, key_type, value_types):
        self.value_types = []
        self.key_type = StorableTypes.from_types(key_type)
        self.key_pos = {}
        self.next_pos = 0
        self.file = file
        self.entry_size = self.key_type.size()

        for t in value_types:
            self.value_types.append(StorableTypes.from_types(t))
        for value_type in self.value_types:
            self.entry_size += value_type.size()
        
    @classmethod
    def new(cls, path, key_type, value_types):
        try:
            file = open(path, 'wb+')
            storage = cls(file, key_type, value_types)
            return storage, storage.get_all_items()
        except:
            return None, None

    def get_entry(self):
        byte_array = bytearray(self.file.read(self.entry_size))
        if len(byte_array) == 0:
            return None
        if len(byte_array) < self.entry_size:
            raise InvalidFile 
        key = self.key_type.from_bytes(remove_bytes(byte_array, self.key_type.size()))
        entry = (key, [])
        for t in self.value_types:
            entry[1].append(t.from_bytes(remove_bytes(byte_array, t.size())).value)
        return entry

    def get_all_entries(self):
        self.file.seek(STARTING_FILE_POS)
        all_entries = {}
        i = 0
        while True:
            entry = self.get_entry()
            if entry == None:
                break
            all_entries[entry[0].value] = entry[1]
            self.key_pos[entry[0]] = i
            i+=1
        self.next_pos = i
        return all_entries

    def get_key_pos(self, key):
        return self.entry_size * self.key_pos[key]

    def get_values_pos(self, key):
        return self.get_key_pos(key) + self.key_type.size()

    def write_key(self, key):
        self.file.seek(self.get_key_pos(key), STARTING_FILE_POS)
        self.file.write(key.to_bytes())
    
    def write_values(self, key, values):
        self.file.seek(self.get_values_pos(key), STARTING_FILE_POS)
        byte_array = bytearray()
        for value in values:
            byte_array.extend(value.to_bytes())
        self.file.write(byte_array)

    def store(self, key, values):
        if (len(values) != len(self.value_types)) and len(values) != 0:
            raise KeysMustBeEqualToValuesOr0
        converted_values = []
        for value, value_type in zip(values, self.value_types):
            converted_values.append(value_type(value))

        key = self.key_type(key)
        pos = self.key_pos.get(key, self.next_pos)
        if pos == self.next_pos:
            self.key_pos[key] = pos
            self.write_key(key)
            self.next_pos+=1
        self.write_values(key, converted_values)
        
if __name__ == '__main__':
    import unittest
    from unittest import TestCase

    from io import BytesIO

    class TestKeyValueStorage(TestCase):
        def str_to_bytes(self, string):
            return bytearray(string.encode()).ljust(FIXED_STR_LEN, STR_PADDING)

        def test_store_on_empty_storage(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, [str, int])
            storage.store("clave", ["valor", 1])
            expected_bytes = self.str_to_bytes("clave")
            expected_bytes.extend(self.str_to_bytes("valor"))
            expected_bytes.extend([0,0,0,1])
            file.seek(STARTING_FILE_POS)
            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave"):0})
            self.assertEqual(storage.next_pos, 1)

        
        def test_store_multiple_on_empty_storage(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, [str, int])
            
            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])

            expected_bytes = self.str_to_bytes("clave1")
            expected_bytes.extend(self.str_to_bytes("valor1"))
            expected_bytes.extend([0,0,0,1])
            expected_bytes.extend(self.str_to_bytes("clave2"))
            expected_bytes.extend(self.str_to_bytes("valor2"))
            expected_bytes.extend([0,0,0,2])

            file.seek(STARTING_FILE_POS)

            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave1"):0, FixedStr("clave2"): 1})
            self.assertEqual(storage.next_pos, 2)

        def test_store_already_existing_key(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, [str, int])
            
            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])
            storage.store("clave1", ["valor3", 3])

            expected_bytes = self.str_to_bytes("clave1")
            expected_bytes.extend(self.str_to_bytes("valor3"))
            expected_bytes.extend([0,0,0,3])
            expected_bytes.extend(self.str_to_bytes("clave2"))
            expected_bytes.extend(self.str_to_bytes("valor2"))
            expected_bytes.extend([0,0,0,2])

            file.seek(STARTING_FILE_POS)

            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave1"):0, FixedStr("clave2"): 1})
            self.assertEqual(storage.next_pos, 2)

        def test_store_all_types(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, [str, int, float, (list, int)])
            storage.store("clave", ["valor", 1, 0.8, [5,5,5,5,5]])

            expected_bytes = bytearray("clave".encode()).ljust(FIXED_STR_LEN, STR_PADDING)
            expected_bytes.extend(bytearray("valor".encode()).ljust(FIXED_STR_LEN, STR_PADDING))
            expected_bytes.extend([0,0,0,1])
            expected_bytes.extend(struct.pack('f', 0.8))
            expected_bytes.extend([5] + [0,0,0,5] * 5 + [0,0,0,0] * (MAX_LEN_LIST - 5))
            
            file.seek(STARTING_FILE_POS)

            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave"):0})
            self.assertEqual(storage.next_pos, 1)

        def test_load_an_entry(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, [str, int])
            
            storage.store("clave", ["valor1", 1])
            file.seek(STARTING_FILE_POS)
            self.assertEqual((FixedStr("clave"), ["valor1",1]), storage.get_entry())
            self.assertEqual(None, storage.get_entry())

        def test_load_all_entries(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(self.str_to_bytes("clave2"))
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            file = BytesIO(initial_bytes)
            storage = KeyValueStorage(file, str, [str, int])

            expected_entries = {
                "clave1": ["valor1", 1],
                "clave2": ["valor2", 2],
            }
            expected_pos = {
                FixedStr("clave1"): 0,
                FixedStr("clave2"): 1,
            }
            entries = storage.get_all_entries()
            self.assertEqual(entries, expected_entries)
            self.assertEqual(storage.key_pos, expected_pos)
            
            self.assertEqual(storage.next_pos, 2)

    unittest.main()