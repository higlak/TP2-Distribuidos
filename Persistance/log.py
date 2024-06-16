from abc import ABC, abstractmethod
from enum import IntEnum
import struct
from Persistance.storage_errors import KeysMustBeEqualToValuesOr0, TooManyValues, UnsupportedType
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import io

VALUES_TYPES_LEN = 1
STRING_LENGTH_BYTES = 2
U32_BYTES = 4
CLIENT_ID_BYTES = 4
LOG_TYPE_BYTES = 1
LEN_LIST_BYTES = 1
FLOAT_BYTES = 4
AMOUNT_OF_ENTRY_BYTES = 2

STR_TYPE_BYTE = 0
INT_TYPE_BYTE = 1
FLOAT_TYPE_BYTE = 2
INT_LIST_TYPE_BYTE = 3

UPDATE = 0
NEW_ENTRY = 1

END_OF_FILE_POS = io.SEEK_END
CURRENT_FILE_POS = io.SEEK_CUR

class LogReadWriter():
    def __init__(self, file):
        self.file = file
    
    @classmethod
    def new(cls, path):
        try:
            file = open(path, 'ab+')
            return cls(file)
        except OSError as e:
            print(f"Error Opening Log: {e}")
            return None

    def log(self, log):
        byte_array = log.get_log_bytes()
        self.file.write(byte_array)
        self.file.flush()

    def read_last_log(self):
        return Log.from_file_pos(self.file, END_OF_FILE_POS)
    
    def read_curr_log(self):
        return Log.from_file_pos(self.file, CURRENT_FILE_POS)
    
    def read_while_log_type(self, log_type):
        logs = [self.read_last_log()]
        while not (logs[-1] is None) and (logs[-1].log_type == log_type):
            logs.append(self.read_curr_log())
        logs.pop()
        return logs

    def read_until_log_type(self, log_type):
        logs = [self.read_last_log()]
        while not (logs[-1] is None) and (logs[-1].log_type != log_type):
            logs.append(self.read_curr_log())
        if logs[-1] == None:
            logs.pop()
        return logs

    def is_type(self, log_type):
        self.log_type == log_type

    def clean(self):
        self.file.truncate(0)
        self.file.flush()

    def close(self):
        self.file.close()

class LogType(IntEnum):
    ChangingFile = 0
    FinishedWriting = 1
    SentBatch = 2
    AckedBatch = 3
    SentFinalResult = 4
    FinishedSendingResults = 5
    FinishedClient = 6

    @classmethod
    def from_file_pos(cls, file, pos):
        file.seek(0, pos)
        if file.tell() == 0:
            return None
        file.seek(-LOG_TYPE_BYTES, CURRENT_FILE_POS)
        log_type_bytes = file.read(LOG_TYPE_BYTES)
        if len(log_type_bytes) == 0:
            return None
        log_type = LogType(byte_array_to_big_endian_integer(log_type_bytes))
        file.seek(-LOG_TYPE_BYTES, CURRENT_FILE_POS)
        return log_type

class Log(ABC):
    def get_log_bytes(self):
        byte_array = self.get_log_arg_bytes()
        byte_array.extend(integer_to_big_endian_byte_array(self.log_type.value, LOG_TYPE_BYTES))
        return byte_array

    @abstractmethod
    def get_log_arg_bytes(self):
        pass

    @classmethod
    def get_log_subclass(self, log_type):
        switch = {
            LogType.ChangingFile: ChangingFile,
            LogType.FinishedWriting: FinishedWriting,
            LogType.SentBatch: SentBatch,
            LogType.AckedBatch: AckedBatch,
            LogType.SentFinalResult: SentFirstFinalResults,
            LogType.FinishedSendingResults: FinishedSendingResults,
            LogType.FinishedClient: FinishedClient,
        }
        return switch[log_type]

    @classmethod
    @abstractmethod
    def from_file_pos(cls, file, pos):
        log_type = LogType.from_file_pos(file, pos)
        if log_type == None:
            return None
        return cls.get_log_subclass(log_type).from_file_pos(file, CURRENT_FILE_POS)
    
    def __eq__(self, other):
        if not isinstance(other, Log):
            return NotImplemented
        return self.log_type == other.log_type and self.params_eq(other)
    
    @abstractmethod
    def params_eq(self, other):
        pass

    def __ne__(self, other):
        return not self.__eq__(other)

class NoArgsLog(Log, ABC):
    def get_log_arg_bytes(self):
        return bytearray([])
    
    @classmethod
    def from_file_pos(cls, file, pos):
        return cls()
    
    def params_eq(self, other):
        return True

#keys = ['key1', 'key2']
#values = [[1,1.1,'hola', 'chau'], [2,2.2,'como', 'estas']]

class ChangingFile(Log):
    def __init__(self, filename, keys, entries= None):
        self.log_type = LogType.ChangingFile
        self.filename = filename
        self.keys = keys
        self.entries = entries
        if len(self._first_entry_value()) > 2**(8* VALUES_TYPES_LEN):
            raise TooManyValues
    
    def get_log_arg_bytes(self):
        byte_array = bytearray()
        if self.entries != None:
            byte_array.extend(self.get_log_update_values_bytes())
        byte_array.extend(self.get_log_type_values_bytes())
        
        new_keys = []
        update_keys = []
        for i in range(len(self.keys)):
            if self.entries == None or self.entries[i] == None:
                new_keys.append(self.keys[i])
            else: 
                update_keys.append(self.keys[i])
        byte_array.extend(get_keys_bytes(update_keys))
        byte_array.extend(get_keys_bytes(new_keys))
        byte_array.extend(get_string_byte_array(self.filename))
        return byte_array
    
    def _first_entry_value(self):
        entry = []
        if self.entries != None:
            for ent in self.entries:
                if ent != None:
                    entry = ent
                    break
        return entry

    def get_log_type_values_bytes(self):
        byte_array = bytearray()
        entry = self._first_entry_value()
        
        for value in entry:
            if type(value) == str:
                byte_array.extend(get_number_byte_array(STR_TYPE_BYTE, VALUES_TYPES_LEN))
            elif type(value) == int:
                byte_array.extend(get_number_byte_array(INT_TYPE_BYTE, VALUES_TYPES_LEN))
            elif type(value) == float:
                byte_array.extend(get_number_byte_array(FLOAT_TYPE_BYTE, VALUES_TYPES_LEN))
            elif type(value) == list and type(value[0]) == int:
                byte_array.extend(get_number_byte_array(INT_LIST_TYPE_BYTE, VALUES_TYPES_LEN))
            else:
                raise UnsupportedType
        byte_array.extend(get_number_byte_array(len(entry), VALUES_TYPES_LEN))
        return byte_array

    def get_log_update_values_bytes(self):
        byte_array = bytearray()
        entry_len = len(self._first_entry_value())

        for i in range(entry_len):
            for values in self.entries:
                if values == None:
                    continue
                if type(values[i]) == str:
                    byte_array.extend(get_string_byte_array(values[i]))
                elif type(values[i]) == int:
                    byte_array.extend(get_number_byte_array(values[i], U32_BYTES))
                elif type(values[i]) == float:
                    byte_array.extend(get_float_byte_array(values[i]))
                elif type(values[i]) == list and type(values[i][0]) == int:
                    byte_array.extend(get_u32list_byte_array(values[i]))
                else:
                    print(f"Value : {values[i]} not supported")
                    raise UnsupportedType
        return byte_array

    @classmethod
    def from_file_pos(cls, file, pos):
        filename = string_from_file_pos(file, pos)
        new_keys = keys_from_file_pos(file, CURRENT_FILE_POS)
        update_keys = keys_from_file_pos(file, CURRENT_FILE_POS)
        amount_of_value_types = numbers_from_file_pos(file, CURRENT_FILE_POS, 1, VALUES_TYPES_LEN)[0]
        if amount_of_value_types == 0:
            return cls(filename, new_keys)
        value_types = numbers_from_file_pos(file, CURRENT_FILE_POS, amount_of_value_types, VALUES_TYPES_LEN)
        values = []
        for value_type in reversed(value_types):
            if value_type == STR_TYPE_BYTE:
                values.insert(0,strings_from_file_pos(file, CURRENT_FILE_POS, len(update_keys)))
            elif value_type == INT_TYPE_BYTE:
                values.insert(0,numbers_from_file_pos(file, CURRENT_FILE_POS, len(update_keys), U32_BYTES))
            elif value_type == FLOAT_TYPE_BYTE:
                values.insert(0,floats_from_file_pos(file, CURRENT_FILE_POS, len(update_keys)))
            elif value_type == INT_LIST_TYPE_BYTE:
                lists = []
                for i in range(len(update_keys)):
                    lists.insert(0, u32_list_from_file_pos(file, CURRENT_FILE_POS))
                values.insert(0, lists)
            else:
                raise UnsupportedType
            
        entries = list(zip(*values))
        keys = update_keys
        for key in new_keys:
            keys.append(key)
            entries.append(None)
        return cls(filename, update_keys, entries)
    
    def params_eq(self, other):
        if self.filename != other.filename or set(self.keys) != set(other.keys):
            return False
        if self.entries == None or other.entries == None:
            if (other.entries == None or all(e is None for e in self.entries)) and (self.entries == None or  all(e is None for e in self.entries)):
                return True
            return False
        for self_values, other_values in zip(self.entries, other.entries):
            if self_values == None or other_values == None:
                if self_values == None and other_values == None:
                    continue
                return False
            for self_value, other_value in zip(self_values, other_values):
                if type(self_value) == float:
                    self_value = get_float_byte_array(self_value)
                    other_value = get_float_byte_array(other_value)
                if self_value != other_value:
                    return False
        return True
        
class FinishedWriting(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.FinishedWriting
    
class SentBatch(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.SentBatch
    
class AckedBatch(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.AckedBatch
    
class SentFirstFinalResults(Log):
    def __init__(self, client_id, n):
        self.log_type = LogType.SentFinalResult
        self.client_id = client_id
        self.n = n

    def get_log_arg_bytes(self):
        byte_array = get_number_byte_array(self.n, U32_BYTES)
        byte_array.extend(get_number_byte_array(self.client_id, CLIENT_ID_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        n, client_id = numbers_from_file_pos(file, pos, 2, CLIENT_ID_BYTES)
        return cls(client_id, n)
    
    def params_eq(self, other):
        return self.n == other.n and self.client_id == other.client_id

class FinishedSendingResults(Log):
    def __init__(self, client_id):
        self.log_type = LogType.FinishedSendingResults
        self.client_id = client_id
    
    def get_log_arg_bytes(self):
        return get_number_byte_array(self.client_id, CLIENT_ID_BYTES)
    
    @classmethod
    def from_file_pos(cls, file, pos):
        return cls(numbers_from_file_pos(file, pos, 1, CLIENT_ID_BYTES)[0])
    
    def params_eq(self, other):
        return self.client_id == other.client_id

class FinishedClient(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.FinishedClient

def string_from_file_pos(file, pos):
    file.seek(-STRING_LENGTH_BYTES, pos)
    len_str = byte_array_to_big_endian_integer(file.read(STRING_LENGTH_BYTES))
    file.seek(-STRING_LENGTH_BYTES-len_str, CURRENT_FILE_POS)
    string = file.read(len_str).decode()
    file.seek(-len_str, CURRENT_FILE_POS)
    return string

def numbers_from_file_pos(file, pos, amount, bytes_per_number):
    file.seek(-bytes_per_number*amount, pos)
    byte_array = bytearray(file.read(bytes_per_number*amount))
    numbers = []
    for _i in range(amount):
        numbers.append(byte_array_to_big_endian_integer(remove_bytes(byte_array, bytes_per_number)))
    file.seek(-bytes_per_number*amount, pos)
    return numbers

def floats_from_file_pos(file, pos, amount):
    file.seek(-FLOAT_BYTES*amount, pos)
    byte_array = bytearray(file.read(FLOAT_BYTES*amount))
    numbers = []
    for _i in range(amount):
        numbers.append(struct.unpack('f', remove_bytes(byte_array,FLOAT_BYTES))[0])
    file.seek(-FLOAT_BYTES*amount, pos)
    return numbers

def amount_of_entries_from_file_pos(file, pos):
    file.seek(-AMOUNT_OF_ENTRY_BYTES, pos)
    amount_of_entries = byte_array_to_big_endian_integer(bytearray(file.read(AMOUNT_OF_ENTRY_BYTES)))
    file.seek(-AMOUNT_OF_ENTRY_BYTES, pos)
    return amount_of_entries

def strings_from_file_pos(file, pos, amount):
    strings = []
    for i in range(amount):
        strings.insert(0, string_from_file_pos(file, pos))
    return strings

def keys_from_file_pos(file, pos):
    amount_of_keys = amount_of_entries_from_file_pos(file, pos)
    return strings_from_file_pos(file, CURRENT_FILE_POS, amount_of_keys)

def u32_list_from_file_pos(file, pos):
    file.seek(-LEN_LIST_BYTES, pos)
    list_len = byte_array_to_big_endian_integer(bytearray(file.read(LEN_LIST_BYTES)))
    file.seek(-LEN_LIST_BYTES, pos)
    return numbers_from_file_pos(file, CURRENT_FILE_POS, list_len, U32_BYTES)


def get_number_byte_array(num, bytes_per_number):
    return integer_to_big_endian_byte_array(num, bytes_per_number)

def get_u32list_byte_array(numbers):
    byte_array = bytearray(b"")
    for num in numbers:
        byte_array.extend(get_number_byte_array(num, U32_BYTES))
    byte_array.extend(integer_to_big_endian_byte_array(len(numbers), LEN_LIST_BYTES))
    return byte_array

def get_float_byte_array(num):
    return bytearray(struct.pack('f',num))

def get_string_byte_array(string):
    string = string[:2**(8*STRING_LENGTH_BYTES)]
    byte_array = bytearray(string.encode())
    byte_array.extend(integer_to_big_endian_byte_array(len(string), STRING_LENGTH_BYTES))
    return byte_array

def get_amount_of_entries_byte_array(l):
    return integer_to_big_endian_byte_array(len(l), AMOUNT_OF_ENTRY_BYTES)

def get_keys_bytes(keys):
    byte_array = bytearray()
    for key in keys:
        byte_array.extend(get_string_byte_array(key))
    byte_array.extend(get_amount_of_entries_byte_array(keys))
    return byte_array

if __name__ == '__main__':
    import unittest
    from unittest import TestCase
    from io import BytesIO
    import pudb; pu.db
    
    class TestLog(TestCase):
        def get_mock_file(self):
            logs_bytes = ChangingFile("file1", ["a", "b"], [["a", 1 , 1, 1.1], ["b", 2, 2, 2.2]]).get_log_bytes()
            logs_bytes.extend(ChangingFile("file1", ["a"], [[1]]).get_log_bytes())
            logs_bytes.extend(ChangingFile("file1", ["a"], [[[1,2,3]]]).get_log_bytes())
            logs_bytes.extend(ChangingFile("file1", ["a", "b"], [[1], None]).get_log_bytes())
            logs_bytes.extend(ChangingFile("file1", ["a", "b"], [None, None]).get_log_bytes())
            logs_bytes.extend(FinishedWriting().get_log_bytes())
            logs_bytes.extend(SentBatch().get_log_bytes())
            logs_bytes.extend(AckedBatch().get_log_bytes())
            logs_bytes.extend(SentFirstFinalResults(1, 2).get_log_bytes())
            logs_bytes.extend(FinishedSendingResults(65536).get_log_bytes())
            logs_bytes.extend(FinishedClient().get_log_bytes())
            return BytesIO(logs_bytes)

        def test_log_to_bytes(self):
            str_bytes = list("a".encode()) + [0,1]
            strb_bytes = list("b".encode()) + [0,1]
            file_bytes = list("file1".encode()) + [0,5]
            float1 = list(get_float_byte_array(1.1))
            float2 = list(get_float_byte_array(2.2))

            self.assertEqual(ChangingFile("file1", ["a", "b"], [["a", 1 , 1, 1.1], ["b", 2, 2, 2.2]]).get_log_bytes(),bytearray(str_bytes + strb_bytes + [0,0,0,1] + [0,0,0,2] + [0,0,0,1] + [0,0,0,2] + float1 + float2 + [STR_TYPE_BYTE,INT_TYPE_BYTE,INT_TYPE_BYTE,FLOAT_TYPE_BYTE] + [4] + str_bytes + strb_bytes + [0,2] + [0,0] + file_bytes + [LogType.ChangingFile]))
            self.assertEqual(ChangingFile("file1", ["a"], [[1]]).get_log_bytes(),bytearray([0,0,0,1] + [INT_TYPE_BYTE] + [1] + str_bytes + [0,1] + [0,0] + file_bytes + [LogType.ChangingFile]))
            self.assertEqual(ChangingFile("file1", ["a"], [[[1,2,3]]]).get_log_bytes(),bytearray([0,0,0,1] + [0,0,0,2] + [0,0,0,3] + [3]+ [INT_LIST_TYPE_BYTE] + [1] + str_bytes + [0,1] + [0,0] + file_bytes + [LogType.ChangingFile]))
            self.assertEqual(ChangingFile("file1", ["a", "b"], [[1], None]).get_log_bytes(),bytearray([0,0,0,1] + [INT_TYPE_BYTE] + [1] + str_bytes + [0,1] + strb_bytes + [0,1] + file_bytes + [LogType.ChangingFile]))
            self.assertEqual(ChangingFile("file1", ["a", "b"], [None, None]).get_log_bytes(),bytearray([0] + [0,0] + str_bytes + strb_bytes + [0,2] + file_bytes + [LogType.ChangingFile]))
            self.assertEqual(FinishedWriting().get_log_bytes(), bytearray([LogType.FinishedWriting.value]))
            self.assertEqual(SentBatch().get_log_bytes(), bytearray([LogType.SentBatch.value]))
            self.assertEqual(AckedBatch().get_log_bytes(), bytearray([LogType.AckedBatch.value]))
            self.assertEqual(SentFirstFinalResults(1, 2).get_log_bytes(), bytearray([0,0,0,2] + [0,0,0,1] + [LogType.SentFinalResult.value]))
            self.assertEqual(FinishedSendingResults(65536).get_log_bytes(), bytearray([0,1,0,0] + [LogType.FinishedSendingResults.value]))
            self.assertEqual(FinishedClient().get_log_bytes(), bytearray([LogType.FinishedClient.value]))

        def test_write_logs(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            logger.log(ChangingFile("file1", ["a", "b"], [["a", 1 , 1, 1.1], ["b", 2, 2, 2.2]]))
            logger.log(ChangingFile("file1", ["a"], [[1]]))
            logger.log(ChangingFile("file1", ["a"], [[[1,2,3]]]))
            logger.log(ChangingFile("file1", ["a", "b"], [[1], None]))
            logger.log(ChangingFile("file1", ["a", "b"], [None, None]))
            logger.log(FinishedWriting())
            logger.log(SentBatch())
            logger.log(AckedBatch())
            logger.log(SentFirstFinalResults(1, 2))
            logger.log(FinishedSendingResults(65536))
            logger.log(FinishedClient())
            mock_file.seek(0)
            self.assertEqual(mock_file.read(1000), self.get_mock_file().read(1000))

        def test_read_empty_log_file(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            self.assertEqual(logger.read_last_log(), None)

        def test_read_log_file(self):
            mock_file = self.get_mock_file()
            logger = LogReadWriter(mock_file)
            self.assertEqual(FinishedClient(), logger.read_last_log())
            self.assertEqual(FinishedSendingResults(65536),logger.read_curr_log())
            self.assertEqual(SentFirstFinalResults(1, 2), logger.read_curr_log())
            self.assertEqual(AckedBatch(), logger.read_curr_log())
            self.assertEqual(SentBatch(), logger.read_curr_log())
            self.assertEqual(FinishedWriting(), logger.read_curr_log())
            self.assertEqual(ChangingFile("file1", ["a", "b"], [None, None]), logger.read_curr_log())
            self.assertEqual(ChangingFile("file1", ["a", "b"], [[1], None]), logger.read_curr_log())
            self.assertEqual(ChangingFile("file1", ["a"], [[[1,2,3]]]), logger.read_curr_log())
            self.assertEqual(ChangingFile("file1", ["a"], [[1]]), logger.read_curr_log())
            self.assertEqual(ChangingFile("file1", ["a", "b"], [["a", 1 , 1, 1.1], ["b", 2, 2, 2.2]]), logger.read_curr_log())
            self.assertAlmostEqual(None, logger.read_curr_log())

        def test_read_until(self):
            mock_file = self.get_mock_file()
            logger = LogReadWriter(mock_file)

            logs = logger.read_until_log_type(LogType.FinishedSendingResults)
            self.assertEqual(logs, [FinishedClient(), FinishedSendingResults(65536)])
        
        def test_read_until_but_log_not_in_file(self):
            file = BytesIO(b"")
            logger = LogReadWriter(file)
            logger.log(FinishedClient())
            logger.log(SentBatch())
            logs = logger.read_until_log_type(LogType.AckedBatch)
            self.assertEqual(logs, [SentBatch(), FinishedClient()])

    unittest.main()