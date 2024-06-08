from abc import ABC, abstractmethod
from enum import IntEnum
import struct
from Persistance.storage_errors import KeysMustBeEqualToValuesOr0
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import io

STRING_LENGTH_BYTES = 1
NUM_BYTES = 4
LOG_TYPE_BYTES = 1
LEN_LIST_BYTES = 1
FLOAT_BYTES = 4
AMOUNT_OF_CONTEXT_ENTRY_BYTES = 2

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
        except:
            return None

    def log(self, log):
        byte_array = log.get_log_bytes()
        self.file.write(byte_array)
        self.file.flush()

    def read_last_log(self):
        return Log.from_file_pos(self.file, END_OF_FILE_POS)
    
    def read_curr_log(self):
        return Log.from_file_pos(self.file, CURRENT_FILE_POS)
    
    def read_until_log_type():
        pass

    def close(self):
        self.file.close()

class LogType(IntEnum):
    ChangedContextStringNum = 0             #string, num
    ChangedContextStringString = 1          #string, string
    ChangedContextStringListU16 = 2         #string, [u16]
    FinishedWriting = 3
    SentBatch = 4
    AckedBatch = 5
    SentFinalResult = 6                     #string
    FinishedSendingResultsOfClient = 7      #num
    FinishedClient = 8

    @classmethod
    def from_file_pos(cls, file, pos):
        file.seek(-LOG_TYPE_BYTES, pos)
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
            LogType.ChangedContextStringNum: ChangeContextFloat,
            LogType.ChangedContextStringString: ChangeContextFloatU32,
            LogType.ChangedContextStringListU16: ChangeContextListU16,
            LogType.FinishedWriting: FinishedWriting,
            LogType.SentBatch: SentBatch,
            LogType.AckedBatch: AckedBatch,
            LogType.SentFinalResult: SentFinalResult,
            LogType.FinishedSendingResultsOfClient: FinishedSendingResultsOfClient,
            LogType.FinishedClient: FinishedClient
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

class ChangeContextFloat(Log):
    def __init__(self, key, num_float):
        self.log_type = LogType.ChangedContextStringNum
        self.key = key
        self.num_float = num_float

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.key)
        byte_array.extend(get_float_byte_array(self.num_float))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        num_float = float_from_file_pos(file, pos)
        string = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(string, num_float)
    
    def params_eq(self, other):
        return self.key == other.key and get_float_byte_array(self.num_float) == get_float_byte_array(other.num_float)
    
class ChangeContextFloatU32(Log):
    def __init__(self, key, num_float, num):
        self.log_type = LogType.ChangedContextStringString
        self.key = key
        self.num_float = num_float
        self.num = num

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.key)
        byte_array.extend(get_float_byte_array(self.num_float))
        byte_array.extend(get_number_byte_array(self.num))
        return byte_array

    @classmethod
    def from_file_pos(cls, file, pos):
        num = numbers_from_file_pos(file, pos, 1)[0]
        num_float = float_from_file_pos(file, CURRENT_FILE_POS)
        key = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(key, num_float, num)
    
    def params_eq(self, other):
        return self.key == other.key and get_float_byte_array(self.num_float) == get_float_byte_array(other.num_float) and self.num == other.num

class ChangeContextListU16(Log):
    def __init__(self, key, numbers):
        self.log_type = LogType.ChangedContextStringListU16
        self.key = key
        self.numbers = numbers

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.key)
        for u16 in self.numbers:
            byte_array.extend(integer_to_big_endian_byte_array(u16, NUM_BYTES))
        byte_array.extend(integer_to_big_endian_byte_array(len(self.numbers), LEN_LIST_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        file.seek(-LEN_LIST_BYTES, pos)
        amount_of_numbers = byte_array_to_big_endian_integer(file.read(LEN_LIST_BYTES))
        file.seek(-LEN_LIST_BYTES, CURRENT_FILE_POS)
        numbers = numbers_from_file_pos(file, CURRENT_FILE_POS, amount_of_numbers)
        string = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(string, numbers)
    
    def params_eq(self, other):
        return self.key == other.key and self.numbers == other.numbers
        
class FinishedWriting(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.FinishedWriting
    
class SentBatch(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.SentBatch
    
class AckedBatch(NoArgsLog):
    def __init__(self):
        self.log_type = LogType.AckedBatch
    
class SentFinalResult(Log):
    def __init__(self, key, client_id):
        self.log_type = LogType.SentFinalResult
        self.key = key
        self.client_id = client_id

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.key)
        byte_array.extend(get_number_byte_array(self.client_id))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        client_id = numbers_from_file_pos(file, pos, 1)[0]
        key = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(key, client_id)
    
    def params_eq(self, other):
        return self.key == other.key and self.client_id == other.client_id

class FinishedSendingResultsOfClient(Log):
    def __init__(self, client_id):
        self.log_type = LogType.FinishedSendingResultsOfClient
        self.client_id = client_id
    
    def get_log_arg_bytes(self):
        return get_number_byte_array(self.client_id)
    
    @classmethod
    def from_file_pos(cls, file, pos):
        return cls(numbers_from_file_pos(file, pos, 1)[0])
    
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

def numbers_from_file_pos(file, pos, amount):
    file.seek(-NUM_BYTES*amount, pos)
    byte_array = bytearray(file.read(NUM_BYTES*amount))
    numbers = []
    for _i in range(amount):
        numbers.append(byte_array_to_big_endian_integer(remove_bytes(byte_array, NUM_BYTES)))
    file.seek(-NUM_BYTES*amount, pos)
    return numbers

def float_from_file_pos(file, pos):
    file.seek(-FLOAT_BYTES, pos)
    num = struct.unpack('f', file.read(FLOAT_BYTES))[0]
    file.seek(-FLOAT_BYTES, pos)
    return num 

def get_number_byte_array(num):
    return integer_to_big_endian_byte_array(num, NUM_BYTES)

def get_u32list_byte_array(numbers):
    byte_array = bytearray(b"")
    for num in numbers:
        byte_array.extend(get_number_byte_array(num))
    byte_array.extend(integer_to_big_endian_byte_array(len(numbers), LEN_LIST_BYTES))
    return byte_array

def get_float_byte_array(num):
    return bytearray(struct.pack('f',num))

def get_string_byte_array(string):
    string = string[:2**(8*STRING_LENGTH_BYTES)]
    byte_array = bytearray(string.encode())
    byte_array.extend(integer_to_big_endian_byte_array(len(string), STRING_LENGTH_BYTES))
    return byte_array

if __name__ == '__main__':
    import unittest
    from unittest import TestCase
    from io import BytesIO
    
    class TestLog(TestCase):
        def get_mock_file(self):
            logs_bytes = ChangeContextFloat("a",1.1).get_log_bytes()
            logs_bytes.extend(ChangeContextFloatU32("a", 1.1, 1).get_log_bytes())
            logs_bytes.extend(ChangeContextListU16("a", [256, 1]).get_log_bytes())
            logs_bytes.extend(FinishedWriting().get_log_bytes())
            logs_bytes.extend(SentBatch().get_log_bytes())
            logs_bytes.extend(AckedBatch().get_log_bytes())
            logs_bytes.extend(SentFinalResult("a", 1).get_log_bytes())
            logs_bytes.extend(FinishedSendingResultsOfClient(65536).get_log_bytes())
            logs_bytes.extend(FinishedClient().get_log_bytes())
            return BytesIO(logs_bytes)

        def test_log_to_bytes(self):
            str_bytes = list("a".encode())
            float_bytes = list(get_float_byte_array(1.1))
            self.assertEqual(ChangeContextFloat("a",1.1).get_log_bytes(), bytearray(str_bytes + [1] + float_bytes + [0]))
            self.assertEqual(ChangeContextFloatU32("a", 1.1, 1).get_log_bytes(), bytearray(str_bytes + [1] + float_bytes + [0,0,0,1] + [1]))
            self.assertEqual(ChangeContextListU16("a", [256, 1]).get_log_bytes(), bytearray(str_bytes + [1] + [0,0,1,0,0,0,0,1,2,2]))
            self.assertEqual(FinishedWriting().get_log_bytes(), bytearray([3]))
            self.assertEqual(SentBatch().get_log_bytes(), bytearray([4]))
            self.assertEqual(AckedBatch().get_log_bytes(), bytearray([5]))
            self.assertEqual(SentFinalResult("a", 1).get_log_bytes(), bytearray(str_bytes + [1] + [0,0,0,1,6]))
            self.assertEqual(FinishedSendingResultsOfClient(65536).get_log_bytes(), bytearray([0,1,0,0,7]))
            self.assertEqual(FinishedClient().get_log_bytes(), bytearray([8]))

        def test_write_logs(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            logger.log(ChangeContextFloat("a",1.1))
            logger.log(ChangeContextFloatU32("a", 1.1, 1))
            logger.log(ChangeContextListU16("a", [256, 1]))
            logger.log(FinishedWriting())
            logger.log(SentBatch())
            logger.log(AckedBatch())
            logger.log(SentFinalResult("a", 1))
            logger.log(FinishedSendingResultsOfClient(65536))
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
            self.assertEqual(FinishedSendingResultsOfClient(65536),logger.read_curr_log())
            self.assertEqual(SentFinalResult("a", 1), logger.read_curr_log())
            self.assertEqual(AckedBatch(), logger.read_curr_log())
            self.assertEqual(SentBatch(), logger.read_curr_log())
            self.assertEqual(FinishedWriting(), logger.read_curr_log())
            self.assertEqual(ChangeContextListU16("a", [256, 1]), logger.read_curr_log())
            self.assertEqual(ChangeContextFloatU32("a", 1.1, 1), logger.read_curr_log())
            self.assertEqual(ChangeContextFloat("a", 1.1), logger.read_curr_log())

    unittest.main()