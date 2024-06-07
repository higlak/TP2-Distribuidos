from abc import ABC, abstractmethod
from enum import IntEnum
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes

STRING_LENGTH_BYTES = 1
NUM_BYTES = 4
LOG_TYPE_BYTES = 1
LEN_LIST_BYTES = 1

END_OF_FILE_POS = 2
CURRENT_FILE_POS = 1        
    
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
        file.seek(-LOG_TYPE_BYTES, pos)
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
            LogType.ChangedContextStringNum: ChangedContextStringNum,
            LogType.ChangedContextStringString: ChangedContextStringString,
            LogType.ChangedContextStringListU16: ChangedContextStringListU16,
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

class ChangedContextStringNum(Log):
    def __init__(self, string, num):
        self.log_type = LogType.ChangedContextStringNum
        self.string = string
        self.num = num

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.string)
        byte_array.extend(integer_to_big_endian_byte_array(self.num, NUM_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        num = numbers_from_file_pos(file, pos, 1)[0]
        string = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(string, num)
    
    def params_eq(self, other):
        return self.string == other.string and self.num == other.num
    
class ChangedContextStringString(Log):
    def __init__(self, string1, string2):
        self.log_type = LogType.ChangedContextStringString
        self.string1 = string1
        self.string2 = string2

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.string1)
        byte_array.extend(get_string_byte_array(self.string2))
        return byte_array

    @classmethod
    def from_file_pos(cls, file, pos):
        string1 = string_from_file_pos(file, pos)
        string2 = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(string1,string2)
    
    def params_eq(self, other):
        return self.string1 == other.string1 and self.string2 == other.string2

class ChangedContextStringListU16(Log):
    def __init__(self, string, numbers):
        self.log_type = LogType.ChangedContextStringListU16
        self.string = string
        self.numbers = numbers

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.string)
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
        return self.string == other.string and self.numbers == other.numbers
        
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
    def __init__(self, string, client_id):
        self.log_type = LogType.SentFinalResult
        self.string = string
        self.client_id = client_id

    def get_log_arg_bytes(self):
        byte_array = get_string_byte_array(self.string)
        byte_array.extend(integer_to_big_endian_byte_array(self.client_id, NUM_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        client_id = numbers_from_file_pos(file, pos, 1)[0]
        string = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(string, client_id)
    
    def params_eq(self, other):
        return self.string == other.string and self.client_id == other.client_id

class FinishedSendingResultsOfClient(Log):
    def __init__(self, client_id):
        self.log_type = LogType.FinishedSendingResultsOfClient
        self.client_id = client_id
    
    def get_log_arg_bytes(self):
        return integer_to_big_endian_byte_array(self.client_id, NUM_BYTES)
    
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
            logs_bytes = ChangedContextStringNum("a",1).get_log_bytes()
            logs_bytes.extend(ChangedContextStringString("a", "a").get_log_bytes())
            logs_bytes.extend(ChangedContextStringListU16("a", [256, 1]).get_log_bytes())
            logs_bytes.extend(FinishedWriting().get_log_bytes())
            logs_bytes.extend(SentBatch().get_log_bytes())
            logs_bytes.extend(AckedBatch().get_log_bytes())
            logs_bytes.extend(SentFinalResult("a", 1).get_log_bytes())
            logs_bytes.extend(FinishedSendingResultsOfClient(65536).get_log_bytes())
            logs_bytes.extend(FinishedClient().get_log_bytes())
            return BytesIO(logs_bytes)

        def test_log_to_bytes(self):
            str_bytes = list("a".encode())
            self.assertEqual(ChangedContextStringNum("a",1).get_log_bytes(), bytearray(str_bytes + [1] + [0,0,0,1,0]))
            self.assertEqual(ChangedContextStringString("a", "a").get_log_bytes(), bytearray(str_bytes + [1] + str_bytes + [1] + [1]))
            self.assertEqual(ChangedContextStringListU16("a", [256, 1]).get_log_bytes(), bytearray(str_bytes + [1] + [0,0,1,0,0,0,0,1,2,2]))
            self.assertEqual(FinishedWriting().get_log_bytes(), bytearray([3]))
            self.assertEqual(SentBatch().get_log_bytes(), bytearray([4]))
            self.assertEqual(AckedBatch().get_log_bytes(), bytearray([5]))
            self.assertEqual(SentFinalResult("a", 1).get_log_bytes(), bytearray(str_bytes + [1] + [0,0,0,1,6]))
            self.assertEqual(FinishedSendingResultsOfClient(65536).get_log_bytes(), bytearray([0,1,0,0,7]))
            self.assertEqual(FinishedClient().get_log_bytes(), bytearray([8]))

        def test_write_logs(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            logger.log(ChangedContextStringNum("a",1))
            logger.log(ChangedContextStringString("a", "a"))
            logger.log(ChangedContextStringListU16("a", [256, 1]))
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
            self.assertEqual(ChangedContextStringListU16("a", [256, 1]), logger.read_curr_log())
            self.assertEqual(ChangedContextStringString("a", "a"), logger.read_curr_log())
            self.assertEqual(ChangedContextStringNum("a",1), logger.read_curr_log())

    unittest.main()