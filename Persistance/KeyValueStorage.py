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

U32_BYTES = 4
LEN_LIST_BYTES = 1
STARTING_FILE_POS = io.SEEK_SET
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
            int: FixedInt,
            (list, int): FixedU32List,
            float: FixedFloat
        }
        storable_t = switch.get(t, None)
        if storable_t == None:
            raise UnsupportedType(f"{t.__name__} not supported")
        return switch[t]

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.value)

class FixedStr(StorableTypes):
    def __init__(self, string, size):
        if not isinstance(string, str):
            raise TypeDoesNotMatchSetType
        self.value = string
        self.size = size
    
    def to_bytes(self):
        return bytearray(self.value.encode()).ljust(self.size, STR_PADDING)
    
    @classmethod
    def from_bytes(self, byte_array):
        return FixedStr(byte_array.strip(STR_PADDING).decode(), len(byte_array))
    
    def __str__(self) -> str:
        return self.value
    
    def __repr__(self):
        return self.value
        

class FixedInt(StorableTypes):
    def __init__(self, num, size):
        if not isinstance(num, int):
            raise TypeDoesNotMatchSetType
        self.value = num
        self.size = size

    def to_bytes(self):
        return integer_to_big_endian_byte_array(self.value, self.size)

    @classmethod
    def from_bytes(self, byte_array):
        return FixedInt(byte_array_to_big_endian_integer(byte_array), len(byte_array))
    
class FixedU32List(StorableTypes):
    def __init__(self, numbers, size):
        if not isinstance(numbers, list):
            raise TypeDoesNotMatchSetType
        for num in numbers:
            if not isinstance(num, int):
                raise TypeDoesNotMatchSetType
        self.value = numbers[:size]
        self.size = size

    def to_bytes(self):
        byte_array = bytearray(integer_to_big_endian_byte_array(len(self.value), LEN_LIST_BYTES))
        numbers = self.value + [0] * (self.size - len(self.value))
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
    
"""
Se le puede unicamente pasar '4' u '8' como size
"""
class FixedFloat(StorableTypes):
    def __init__(self, number, size):
        if not isinstance(number, float):
            raise TypeDoesNotMatchSetType
        self.value = number
        if size == 4:
            self.size = 'f'
        elif size == 8:
            self.size = 'd'
        else:
            raise UnsupportedType

    def to_bytes(self):
        return bytearray(struct.pack(self.size,self.value))

    @classmethod
    def from_bytes(self, byte_array):
        return FixedFloat(struct.unpack(self.size, byte_array)[0])

class KeyValueStorage():
    def __init__(self, file, key_type, fixed_key_size, value_types, values_fixed_size):
        self.value_types = []
        self.value_sizes = values_fixed_size
        self.key_type = StorableTypes.from_types(key_type)
        self.fixed_key_size = fixed_key_size
        self.key_pos = {}
        self.next_pos = 0
        self.file = file
        self.entry_size = self.fixed_key_size

        for t in value_types:
            self.value_types.append(StorableTypes.from_types(t))
        for value_size in self.value_sizes:
            self.entry_size += value_size
        
    @classmethod
    def new(cls, path, key_type, fixed_key_size, value_types, values_fixed_size):
        try:
            file = open(path, 'rb+')
        except FileNotFoundError:
            file = open(path, 'wb+')
        except OSError as e:
            print(f"Error Opening Storage: {e}")
            return None, None
        
        storage = cls(file, key_type, fixed_key_size, value_types, values_fixed_size)
        return storage, storage.get_all_entries()

    def get_entry(self):
        byte_array = bytearray(self.file.read(self.entry_size))
        if len(byte_array) == 0:
            return None
        if len(byte_array) < self.entry_size:
            raise InvalidFile 
        key = self.key_type.from_bytes(remove_bytes(byte_array, self.fixed_key_size))
        values = []
        for value_type, value_size in zip(self.value_types, self.value_sizes):
            values.append(value_type.from_bytes(remove_bytes(byte_array, value_size)).value)
        if len(values) == 1:
            return key, values[0]
        return key, values

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
        return self.get_key_pos(key) + self.fixed_key_size

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
        if type(values) != list:
            values = [values]
        if (len(values) != len(self.value_types)) and len(values) != 0:
            raise KeysMustBeEqualToValuesOr0
        converted_values = []
        for value, value_type, value_size in zip(values, self.value_types, self.value_sizes):
            converted_values.append(value_type(value, value_size))

        key = self.key_type(key, self.fixed_key_size)
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

    MAX_LEN_LIST = 10
    FIXED_FLOAT_BYTES = 4
    FIXED_STR_LEN = 128

    class TestKeyValueStorage(TestCase):
        def str_to_bytes(self, string):
            return bytearray(string.encode()).ljust(FIXED_STR_LEN, STR_PADDING)

        def test_store_on_empty_storage(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
            storage.store("clave", ["valor", 1])
            expected_bytes = self.str_to_bytes("clave")
            expected_bytes.extend(self.str_to_bytes("valor"))
            expected_bytes.extend([0,0,0,1])
            file.seek(STARTING_FILE_POS)
            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave", FIXED_STR_LEN):0})
            self.assertEqual(storage.next_pos, 1)

        
        def test_store_multiple_on_empty_storage(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
            
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
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN):0, FixedStr("clave2", FIXED_STR_LEN): 1})
            self.assertEqual(storage.next_pos, 2)

        def test_store_already_existing_key(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
            
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
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN):0, FixedStr("clave2", FIXED_STR_LEN): 1})
            self.assertEqual(storage.next_pos, 2)

        def test_store_all_types(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int, float, (list, int)], [FIXED_STR_LEN, 8, FIXED_FLOAT_BYTES, MAX_LEN_LIST])
            storage.store("clave", ["valor", 1, 0.8, [5,5,5,5,5]])

            expected_bytes = bytearray("clave".encode()).ljust(FIXED_STR_LEN, STR_PADDING)
            expected_bytes.extend(bytearray("valor".encode()).ljust(FIXED_STR_LEN, STR_PADDING))
            expected_bytes.extend([0,0,0,0,0,0,0,1])
            expected_bytes.extend(struct.pack('f', 0.8))
            expected_bytes.extend([5] + [0,0,0,5] * 5 + [0,0,0,0] * (MAX_LEN_LIST - 5))
            
            file.seek(STARTING_FILE_POS)

            self.assertEqual(expected_bytes, file.read(1000))
            self.assertEqual(storage.key_pos, {FixedStr("clave", FIXED_STR_LEN):0})
            self.assertEqual(storage.next_pos, 1)

        def test_load_an_entry(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
            
            storage.store("clave", ["valor1", 1])
            file.seek(STARTING_FILE_POS)
            self.assertEqual((FixedStr("clave", FIXED_STR_LEN), ["valor1",1]), storage.get_entry())
            self.assertEqual(None, storage.get_entry())

        def test_load_all_entries(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(self.str_to_bytes("clave2"))
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            file = BytesIO(initial_bytes)
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])

            expected_entries = {
                "clave1": ["valor1", 1],
                "clave2": ["valor2", 2],
            }
            expected_pos = {
                FixedStr("clave1", FIXED_STR_LEN): 0,
                FixedStr("clave2", FIXED_STR_LEN): 1,
            }
            entries = storage.get_all_entries()
            self.assertEqual(entries, expected_entries)
            self.assertEqual(storage.key_pos, expected_pos)
            
            self.assertEqual(storage.next_pos, 2)

    unittest.main()