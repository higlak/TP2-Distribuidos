from abc import ABC, abstractmethod
import os
import struct
from Persistance.storage_errors import *
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import io

U32_BYTES = 4
LEN_LIST_BYTES = 1
STARTING_FILE_POS = io.SEEK_SET
LAST_FILE_POS = io.SEEK_END
STR_PADDING = bytes([0xff])
CROSS_OUT_BYTE = bytes([0xfe])

class StorableTypes(ABC):
    @abstractmethod
    def to_bytes(self):
        pass

    @classmethod
    @abstractmethod
    def from_bytes(cls, byte_array):
        pass

    @classmethod
    @abstractmethod
    def get_size_in_bytes(cls, size):
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
        if type(string) != str:
            raise TypeDoesNotMatchSetType
        self.value = string
        self.size = size
    
    def to_bytes(self):
        return bytearray(self.value.encode()).ljust(self.size, STR_PADDING)[:self.size]
    
    @classmethod
    def from_bytes(self, byte_array):
        return FixedStr(byte_array.strip(STR_PADDING).decode(), len(byte_array))
    
    def __str__(self) -> str:
        return self.value
    
    def __repr__(self):
        return self.value
    
    @classmethod
    def get_size_in_bytes(cls, size):
        return size
        

class FixedInt(StorableTypes):
    def __init__(self, num, size):
        if type(num) != int:
            raise TypeDoesNotMatchSetType
        self.value = num
        self.size = size

    def to_bytes(self):
        return integer_to_big_endian_byte_array(self.value, self.size)

    @classmethod
    def from_bytes(self, byte_array):
        return FixedInt(byte_array_to_big_endian_integer(byte_array), len(byte_array))
    
    @classmethod
    def get_size_in_bytes(cls, size):
        return size

class FixedU32List(StorableTypes):
    def __init__(self, numbers, size):
        if type(numbers) != list:
            raise TypeDoesNotMatchSetType
        for num in numbers:
            if type(num) != int:
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
        leftover_nums = 0
        while len(byte_array) > U32_BYTES:
            if len(numbers) < len_numbers:
                numbers.append(byte_array_to_big_endian_integer(remove_bytes(byte_array,U32_BYTES)))
            else:
                leftover_nums = int(len(byte_array)/4) 
                break
        return FixedU32List(numbers, len_numbers + leftover_nums)
    
    @classmethod
    def get_size_in_bytes(cls, size):
        return size * U32_BYTES + LEN_LIST_BYTES
    
"""
Se le puede unicamente pasar '4' u '8' como size
"""
class FixedFloat(StorableTypes):
    def __init__(self, number, size):
        if type(number) != float:
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
    def from_bytes(cls, byte_array):
        if len(byte_array) == 4:
            size = 'f'
        elif len(byte_array) == 8:
            size = 'd'
        else:
            raise InvalidFile
            
        return FixedFloat(struct.unpack(size, byte_array)[0], len(byte_array))
    
    @classmethod
    def get_size_in_bytes(cls, size):
        return size
    
    def __eq__(self, other):
        return self.to_bytes() == other.to_bytes()

class KeyValueStorage():
    def __init__(self, file, key_type, fixed_key_size, value_types, values_fixed_size):
        self.value_types = []
        self.value_sizes = values_fixed_size
        self.key_type = StorableTypes.from_types(key_type)
        self.fixed_key_size = fixed_key_size
        self.key_pos = {}
        self.next_pos = 0
        self.file = file
        self.entry_byte_size = self.key_type.get_size_in_bytes(fixed_key_size)

        for t, size in zip(value_types, values_fixed_size):
            value_type = StorableTypes.from_types(t)
            self.value_types.append(value_type)
            self.entry_byte_size += value_type.get_size_in_bytes(size)
        
    @classmethod
    def new(cls, path, key_type, fixed_key_size, value_types, values_fixed_size):
        if type(value_types) != list or type(values_fixed_size) != list:
            return None, None
        try:
            file = open(path, 'rb+')
        except FileNotFoundError:
            file = open(path, 'wb+')
        except OSError as e:
            print(f"Error Opening Storage: {e}")
            return None, None

        storage = cls(file, key_type, fixed_key_size, value_types, values_fixed_size)
        return storage, storage.get_all_entries()

    def is_mid_removal_entry_bytes(self, entry_bytes):
        return CROSS_OUT_BYTE[0] in entry_bytes

    def get_entry(self):
        byte_array = bytearray(self.file.read(self.entry_byte_size))
        if len(byte_array) == 0:
            print("chau")
            return None
        if len(byte_array) < self.entry_byte_size:
            self.del_last_bytes(len(byte_array))
            os.sync(self.file.fileno())
            return None

        key_bytes = remove_bytes(byte_array, self.key_type.get_size_in_bytes(self.fixed_key_size))
        if self.is_mid_removal_entry_bytes(key_bytes):
            return self.get_entry()
        key = self.key_type.from_bytes(key_bytes)
        
        values = []
        for value_type, value_size in zip(self.value_types, self.value_sizes):
            values.append(value_type.from_bytes(remove_bytes(byte_array, value_type.get_size_in_bytes(value_size))).value)
        if len(values) == 1:
            return key, values[0]
        return key, values

    def get_all_entries(self):
        self.file.seek(STARTING_FILE_POS)
        all_entries = {}
        i = 0
        while True:
            #print(i)
            if i == 1068:
                pass
            entry = self.get_entry()
            if entry == None:
                break
            all_entries[entry[0].value] = entry[1]
            self.key_pos[entry[0]] = self.key_pos.get(entry[0],i)
            if self.key_pos[entry[0]] == i:
                i+=1
        self.next_pos = i
        return all_entries

    def get_key_pos(self, key):
        return self.entry_byte_size * self.key_pos[key]

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

    def store_all(self, keys, list_of_values):
        for key, values in zip(keys, list_of_values):
            self.store(key, values)

    def _convert_values(self, values):
        converted_values = []
        for value, value_type, value_size in zip(values, self.value_types, self.value_sizes):
            converted_values.append(value_type(value, value_size))
        return converted_values

    def store(self, key, values): 
        if (len(values) != len(self.value_types)) and len(values) != 0 and values != [list]:
            print(f"values: {values} , values_types: {self.value_types}")
            raise KeysMustBeEqualToValuesOr0
        
        converted_values = self._convert_values(values)

        key = self.key_type(key, self.fixed_key_size)
        pos = self.key_pos.get(key, self.next_pos)
        self._store_in_pos(pos, key, converted_values)
        
    def _store_in_pos(self, pos, key, values, stepping=False):
        if pos == self.next_pos:
            self.key_pos[key] = pos
            self.next_pos+=1
            self.write_key(key)
        elif stepping:
            self.write_key(key)

        self.write_values(key, values)
        self.file.flush()
        os.fsync(self.file.fileno())

    def remove(self, key):
        key = self.key_type(key, self.fixed_key_size)
        if key not in self.key_pos:
            return
        
        self.next_pos -=1
        pos_to_remove = self.key_pos.pop(key)
        if self.next_pos != pos_to_remove:
            self.cross_out_key(pos_to_remove)

            self.file.seek(self.next_pos * self.entry_byte_size, STARTING_FILE_POS)
            entry = self.get_entry()
            last_key = entry[0].value
            last_values = entry[1]

            if len(self.value_types) == 1:
                last_values = [last_values]
            last_values = self._convert_values(last_values)
            last_key = self.key_type(last_key, self.fixed_key_size)
            self.key_pos[last_key] = pos_to_remove
            self._store_in_pos(pos_to_remove, last_key, last_values, stepping=True)
        self.del_last_line()
        
        os.fsync(self.file.fileno())

    def cross_out_key(self, key_pos):
        self.file.seek(key_pos*self.entry_byte_size, STARTING_FILE_POS)
        self.file.write(CROSS_OUT_BYTE *self.fixed_key_size)
        self.file.flush()

    def del_last_bytes(self, amount_of_bytes):
        size = self.file.seek(0, LAST_FILE_POS)
        self.file.truncate(max(0,size - amount_of_bytes))
        self.file.flush()

    def del_last_line(self):
        self.del_last_bytes(self.entry_byte_size)

    def close(self):
        self.file.close()

    def delete(self):
        self.file.close()
        filename = self.file.name
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
        except:
            print("Could not remove ", filename)
        
if __name__ == '__main__':
    import unittest
    from unittest import TestCase

    from io import BytesIO
    import pudb; pu.db

    MAX_LEN_LIST = 10
    FIXED_FLOAT_BYTES = 4
    FIXED_STR_LEN = 512
    ALL = 100000

    class TestKeyValueStorage(TestCase):
        def str_to_bytes(self, string):
            return bytearray(string.encode()).ljust(FIXED_STR_LEN, STR_PADDING)[:self.size]

        def test_store_on_empty_storage(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
            storage.store("clave", ["valor", 1])
            expected_bytes = self.str_to_bytes("clave")
            expected_bytes.extend(self.str_to_bytes("valor"))
            expected_bytes.extend([0,0,0,1])
            file.seek(STARTING_FILE_POS)
            self.assertEqual(expected_bytes, file.read(ALL))
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

            self.assertEqual(expected_bytes, file.read(ALL))
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

            self.assertEqual(expected_bytes, file.read(ALL))
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN):0, FixedStr("clave2", FIXED_STR_LEN): 1})
            self.assertEqual(storage.next_pos, 2)

        def test_store_all_types(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int, float, (list, int)], [FIXED_STR_LEN, 8, FIXED_FLOAT_BYTES, MAX_LEN_LIST])
            storage.store("clave", ["valor", 1, 0.8, [5,5,5,5,5]])

            expected_bytes = self.str_to_bytes("clave")
            expected_bytes.extend(self.str_to_bytes("valor"))
            expected_bytes.extend([0,0,0,0,0,0,0,1])
            expected_bytes.extend(struct.pack('f', 0.8))
            expected_bytes.extend([5] + [0,0,0,5] * 5 + [0,0,0,0] * (MAX_LEN_LIST - 5))
            
            file.seek(STARTING_FILE_POS)
            
            self.assertEqual(expected_bytes, file.read(ALL))
            self.assertEqual(storage.key_pos, {FixedStr("clave", FIXED_STR_LEN):0})
            self.assertEqual(storage.next_pos, 1)

        def test_load_an_entry(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [(list, int), float, str, int], [MAX_LEN_LIST, FIXED_FLOAT_BYTES, FIXED_STR_LEN, U32_BYTES])
            
            storage.store("clave", [[5,5,5,5,5], 0.1,"valor1", 1])
            file.seek(STARTING_FILE_POS)


            (key, values) = storage.get_entry()
            
            self.assertEqual(key, FixedStr("clave", FIXED_STR_LEN))
            self.assertEqual([5,5,5,5,5], values[0])
            self.assertEqual(FixedFloat(0.1, 4), FixedFloat(values[1], 4))
            self.assertEqual("valor1", values[2])
            self.assertEqual(1, values[3])
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

        def test_remove_not_found_key(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_bytes = self.str_to_bytes("clave1")
            expected_bytes.extend(self.str_to_bytes("valor1"))
            expected_bytes.extend([0,0,0,1])
            expected_bytes.extend(self.str_to_bytes("clave2"))
            expected_bytes.extend(self.str_to_bytes("valor2"))
            expected_bytes.extend([0,0,0,2])

            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])
            storage.remove("no key")

            file.seek(0,STARTING_FILE_POS)
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN) :0, FixedStr("clave2", FIXED_STR_LEN):1})
            self.assertEqual(storage.next_pos, 2)
            self.assertEqual(expected_bytes, file.read(ALL))

        def test_remove_last_key(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_bytes = self.str_to_bytes("clave1")
            expected_bytes.extend(self.str_to_bytes("valor1"))
            expected_bytes.extend([0,0,0,1])

            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])
            storage.remove("clave2")

            file.seek(0,STARTING_FILE_POS)
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN) :0})
            self.assertEqual(storage.next_pos, 1)
            self.assertEqual(expected_bytes, file.read(ALL))

        def test_remove_first_key(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_bytes = self.str_to_bytes("clave2")
            expected_bytes.extend(self.str_to_bytes("valor2"))
            expected_bytes.extend([0,0,0,2])

            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])
            storage.remove("clave1")

            file.seek(0,STARTING_FILE_POS)
            self.assertEqual(storage.key_pos, {FixedStr("clave2", FIXED_STR_LEN) :0})
            self.assertEqual(storage.next_pos, 1)
            self.assertEqual(expected_bytes, file.read(ALL))

        def test_remove_key_in_the_middle(self):
            file = BytesIO(b"")
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_bytes = self.str_to_bytes("clave1")
            expected_bytes.extend(self.str_to_bytes("valor1"))
            expected_bytes.extend([0,0,0,1])
            expected_bytes.extend(self.str_to_bytes("clave3"))
            expected_bytes.extend(self.str_to_bytes("valor3"))
            expected_bytes.extend([0,0,0,3])

            storage.store("clave1", ["valor1", 1])
            storage.store("clave2", ["valor2", 2])
            storage.store("clave3", ["valor3", 3])
            storage.remove("clave2")

            file.seek(0,STARTING_FILE_POS)
            self.assertEqual(storage.key_pos, {FixedStr("clave1", FIXED_STR_LEN) :0, FixedStr("clave3", FIXED_STR_LEN):1})
            self.assertEqual(storage.next_pos, 2)
            self.assertEqual(expected_bytes, file.read(ALL))

        def test_uncompleted_remove_full_cross_out(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(CROSS_OUT_BYTE * FIXED_STR_LEN)
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            initial_bytes.extend(self.str_to_bytes("clave3"))
            initial_bytes.extend(self.str_to_bytes("valor3"))
            initial_bytes.extend([0,0,0,3])
            file = BytesIO(initial_bytes)
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_entries = {
                "clave1": ["valor1", 1],
                "clave3": ["valor3", 3],
            }
            expected_pos = {
                FixedStr("clave1", FIXED_STR_LEN): 0,
                FixedStr("clave3", FIXED_STR_LEN): 1,
            }
            entries = storage.get_all_entries()
            self.assertEqual(entries, expected_entries)
            self.assertEqual(storage.key_pos, expected_pos)
            
            self.assertEqual(storage.next_pos, 2)
        
        def test_uncompleted_remove_partial_cross_out(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(CROSS_OUT_BYTE * 2 + self.str_to_bytes("clave2")[2:])
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            initial_bytes.extend(self.str_to_bytes("clave3"))
            initial_bytes.extend(self.str_to_bytes("valor3"))
            initial_bytes.extend([0,0,0,3])
            file = BytesIO(initial_bytes)
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_entries = {
                "clave1": ["valor1", 1],
                "clave3": ["valor3", 3],
            }
            expected_pos = {
                FixedStr("clave1", FIXED_STR_LEN): 0,
                FixedStr("clave3", FIXED_STR_LEN): 1,
            }
            entries = storage.get_all_entries()
            self.assertEqual(entries, expected_entries)
            self.assertEqual(storage.key_pos, expected_pos)
            
            self.assertEqual(storage.next_pos, 2)

        def test_uncompleted_remove_dup_key(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(self.str_to_bytes("clave3"))
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            initial_bytes.extend(self.str_to_bytes("clave3"))
            initial_bytes.extend(self.str_to_bytes("valor3"))
            initial_bytes.extend([0,0,0,3])
            file = BytesIO(initial_bytes)
            storage = KeyValueStorage(file, str, FIXED_STR_LEN, [str, int], [FIXED_STR_LEN, U32_BYTES])
        
            expected_entries = {
                "clave1": ["valor1", 1],
                "clave3": ["valor3", 3],
            }
            expected_pos = {
                FixedStr("clave1", FIXED_STR_LEN): 0,
                FixedStr("clave3", FIXED_STR_LEN): 1,
            }
            entries = storage.get_all_entries()
            self.assertEqual(entries, expected_entries)
            self.assertEqual(storage.key_pos, expected_pos)
            
            self.assertEqual(storage.next_pos, 2)

        def test_uncompleted_key_entry_insertion(self):
            initial_bytes = self.str_to_bytes("clave1")
            initial_bytes.extend(self.str_to_bytes("valor1"))
            initial_bytes.extend([0,0,0,1])
            initial_bytes.extend(self.str_to_bytes("clave2"))
            initial_bytes.extend(self.str_to_bytes("valor2"))
            initial_bytes.extend([0,0,0,2])
            initial_bytes.extend("cla".encode())
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
            file.seek(-3, LAST_FILE_POS)
            self.assertEqual(file.read(3), bytearray([0,0,2]))
    
    unittest.main()