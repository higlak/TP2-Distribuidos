from abc import ABC, abstractmethod
from enum import IntEnum
import struct
from Persistance.storage_errors import KeysMustBeEqualToValuesOr0
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes
import io

STRING_LENGTH_BYTES = 1
U32_BYTES = 4
CLIENT_ID_BYTES = 4
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
    
    def read_until_log_type(self, log_type):
        logs = [self.read_last_log()]
        while not (logs[-1] is None) and (logs[-1].log_type != log_type):
            logs.append(self.read_curr_log())
        if logs[-1] == None:
            logs.pop()
        return logs

    def close(self):
        self.file.close()

class LogType(IntEnum):
    ChangeContextFloat = 0             
    ChangeContextFloatU32 = 1          
    ChangeContextListU16 = 2
    ChangeContextNoArgs = 3
    FinishedWriting = 4
    SentBatch = 5
    AckedBatch = 6
    SentFinalResult = 7
    FinishedSendingResultsOfClient = 8
    FinishedClient = 9
    ChangedMetadata = 10
    

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
            LogType.ChangeContextFloat: ChangeContextFloat,
            LogType.ChangeContextFloatU32: ChangeContextFloatU32,
            LogType.ChangeContextListU16: ChangeContextListU16,
            LogType.ChangeContextNoArgs: ChangeContextNoArgs,
            LogType.FinishedWriting: FinishedWriting,
            LogType.SentBatch: SentBatch,
            LogType.AckedBatch: AckedBatch,
            LogType.SentFinalResult: SentFinalResult,
            LogType.FinishedSendingResultsOfClient: FinishedSendingResultsOfClient,
            LogType.FinishedClient: FinishedClient,
            LogType.ChangedMetadata: ChangeMetadata,
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

class ChangeContextLog(Log, ABC):
    def __init__(self, client_id, keys):
        self.client_id = client_id
        self.keys = keys
    
    def get_log_arg_bytes(self):
        byte_array = self.get_log_values_bytes()
        byte_array.extend(get_keys_bytes(self.keys))
        byte_array.extend(get_number_byte_array(self.client_id, CLIENT_ID_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        client_id = numbers_from_file_pos(file, pos, 1, CLIENT_ID_BYTES)[0]
        keys = keys_from_file_pos(file, CURRENT_FILE_POS)
        return cls.new_values_from_file_pos(file, CURRENT_FILE_POS, client_id, keys)
    
    def params_eq(self, other):
        if self.client_id != other.client_id or self.keys != other.keys:
            return False
        return self.values_eq(other)
    
    @classmethod
    @abstractmethod
    def from_keys_values(cls, client_id, keys, list_of_values):
        pass

    @abstractmethod
    def get_log_values_bytes(self):
        pass
    
    @classmethod
    @abstractmethod
    def new_values_from_file_pos(file, pos, client_id, keys):
        pass
    
    @abstractmethod
    def values_eq(self, other):
        pass

class ChangeContextNoArgs(ChangeContextLog):
    def __init__(self, client_id, keys):
        super().__init__(client_id, keys)
        self.log_type = LogType.ChangeContextNoArgs

    @classmethod
    def from_keys_values(cls, client_id, keys, list_of_values):
        return cls(client_id, keys)

    def get_log_values_bytes(self):
        return bytearray()
    
    @classmethod
    def new_values_from_file_pos(cls, file, pos, client_id, keys):
        return cls(client_id, keys)
    
    def values_eq(self, other):
        return True
        
class ChangeContextFloat(ChangeContextLog):
    def __init__(self, client_id, keys, float_numbers):
        super().__init__(client_id, keys)
        self.log_type = LogType.ChangeContextFloat
        self.float_numbers = float_numbers

    @classmethod
    def from_keys_values(cls, client_id, keys, list_of_values):
        return cls(client_id, keys, list_of_values)

    def get_log_values_bytes(self):
        byte_array = bytearray()
        for num in self.float_numbers:
            byte_array.extend(get_float_byte_array(num))
        return byte_array
    
    @classmethod
    def new_values_from_file_pos(cls, file, pos, client_id, keys):
        float_numbers = floats_from_file_pos(file, pos, len(keys))
        return cls(client_id, keys, float_numbers)
    
    def values_eq(self, other):
        for self_num, other_num in zip(self.float_numbers, other.float_numbers):
            if get_float_byte_array(self_num) != get_float_byte_array(other_num):
                return False
        return True
    
class ChangeMetadata(Log):
    def __init__(self, keys, numbers):
        self.log_type = LogType.ChangedMetadata
        self.keys = keys
        self.numbers = numbers

    def get_log_arg_bytes(self):
        byte_array = bytearray()
        for num in self.numbers:
            byte_array.extend(get_number_byte_array(num, U32_BYTES))

        byte_array.extend(get_keys_bytes(self.keys))
        return byte_array

    @classmethod
    def from_file_pos(cls, file, pos):
        keys = keys_from_file_pos(file, pos)
        numbers = numbers_from_file_pos(file, CURRENT_FILE_POS, len(keys), U32_BYTES)
        return cls(keys, numbers)
    
    def params_eq(self, other):
        return self.keys == other.keys and self.numbers == other.numbers

class ChangeContextFloatU32(ChangeContextLog):
    def __init__(self,client_id, keys, float_numbers, numbers):
        super().__init__(client_id, keys)
        self.log_type = LogType.ChangeContextFloatU32
        self.float_numbers = float_numbers
        self.numbers = numbers

    @classmethod
    def from_keys_values(cls, client_id, keys, list_of_values):
        numbers = []
        float_numbers = []
        for values in list_of_values:
            numbers.append(values[0])
            float_numbers.append(values[1])
        return cls(client_id, keys, float_numbers, numbers)

    def get_log_values_bytes(self):
        byte_array = bytearray()
        for num in self.numbers:
            byte_array.extend(get_number_byte_array(num, U32_BYTES))
        for num in self.float_numbers:
            byte_array.extend(get_float_byte_array(num))
        return byte_array
    
    @classmethod
    def new_values_from_file_pos(cls, file, pos, client_id, keys):
        float_numbers = floats_from_file_pos(file, pos, len(keys))
        numbers = numbers_from_file_pos(file, CURRENT_FILE_POS, len(keys), U32_BYTES)
        return cls(client_id, keys, float_numbers, numbers)
    
    def values_eq(self, other):
        if self.numbers != other.numbers:
            return False
        for self_num, other_num in zip(self.float_numbers, other.float_numbers):
            if get_float_byte_array(self_num) != get_float_byte_array(other_num):
                return False
        return True

class ChangeContextListU16(ChangeContextLog):
    def __init__(self, client_id, keys, numbers_lists):
        super().__init__(client_id, keys)
        self.log_type = LogType.ChangeContextListU16
        self.numbers_lists = numbers_lists

    @classmethod
    def from_keys_values(cls, client_id, keys, list_of_values):
        return cls(client_id, keys, list_of_values)

    def get_log_values_bytes(self):
        byte_array = bytearray()
        for l in self.numbers_lists:
            byte_array.extend(get_u32list_byte_array(l))
        return byte_array
    
    @classmethod
    def new_values_from_file_pos(cls, file, pos, client_id, keys):
        numbers_lists = []
        for l in range(len(keys)):
            file.seek(-LEN_LIST_BYTES, pos)
            amount_of_numbers = byte_array_to_big_endian_integer(file.read(LEN_LIST_BYTES))
            file.seek(-LEN_LIST_BYTES, CURRENT_FILE_POS)
            numbers_lists.insert(0, numbers_from_file_pos(file, CURRENT_FILE_POS, amount_of_numbers, U32_BYTES))
        return cls(client_id, keys, numbers_lists)
    
    def values_eq(self, other):
        return self.numbers_lists == other.numbers_lists
        
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
        byte_array.extend(get_number_byte_array(self.client_id, CLIENT_ID_BYTES))
        return byte_array
    
    @classmethod
    def from_file_pos(cls, file, pos):
        client_id = numbers_from_file_pos(file, pos, 1, CLIENT_ID_BYTES)[0]
        key = string_from_file_pos(file, CURRENT_FILE_POS)
        return cls(key, client_id)
    
    def params_eq(self, other):
        return self.key == other.key and self.client_id == other.client_id

class FinishedSendingResultsOfClient(Log):
    def __init__(self, client_id):
        self.log_type = LogType.FinishedSendingResultsOfClient
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
    file.seek(-AMOUNT_OF_CONTEXT_ENTRY_BYTES, pos)
    amount_of_entries = byte_array_to_big_endian_integer(bytearray(file.read(AMOUNT_OF_CONTEXT_ENTRY_BYTES)))
    file.seek(-AMOUNT_OF_CONTEXT_ENTRY_BYTES, pos)
    return amount_of_entries

def keys_from_file_pos(file, pos):
    amount_of_keys = amount_of_entries_from_file_pos(file, pos)
    keys = []
    for i in range(amount_of_keys):
        keys.insert(0, string_from_file_pos(file, CURRENT_FILE_POS))
    return keys

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
    return integer_to_big_endian_byte_array(len(l), AMOUNT_OF_CONTEXT_ENTRY_BYTES)

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
    #import pudb; pu.db
    
    class TestLog(TestCase):
        def get_mock_file(self):
            logs_bytes = ChangeContextFloat(1,["a"],[1.1]).get_log_bytes()
            logs_bytes.extend(ChangeContextFloatU32(1,["a", "b"], [1.1, 2.2], [1,2]).get_log_bytes())
            logs_bytes.extend(ChangeContextListU16(1,["a", "b"], [[5,5],[256, 1]]).get_log_bytes())
            logs_bytes.extend(ChangeContextNoArgs(1,["a"]).get_log_bytes())
            logs_bytes.extend(FinishedWriting().get_log_bytes())
            logs_bytes.extend(SentBatch().get_log_bytes())
            logs_bytes.extend(AckedBatch().get_log_bytes())
            logs_bytes.extend(SentFinalResult("a", 1).get_log_bytes())
            logs_bytes.extend(FinishedSendingResultsOfClient(65536).get_log_bytes())
            logs_bytes.extend(FinishedClient().get_log_bytes())
            logs_bytes.extend(ChangeMetadata(["a"],[2]).get_log_bytes())
            return BytesIO(logs_bytes)

        def test_log_to_bytes(self):
            str_bytes = list("a".encode()) + [1]
            strb_bytes = list("b".encode()) + [1]
            float1 = list(get_float_byte_array(1.1))
            float2 = list(get_float_byte_array(2.2))

            float_bytes = list(get_float_byte_array(1.1))
            self.assertEqual(ChangeContextFloat(1, ["a"],[1.1]).get_log_bytes(), bytearray(float_bytes + str_bytes + [0,1] + [0,0,0,1] + [LogType.ChangeContextFloat.value]))
            self.assertEqual(ChangeContextFloatU32(1, ["a", "b"], [1.1, 2.2], [1,2]).get_log_bytes(), bytearray([0,0,0,1]+ [0,0,0,2] + float1 + float2 + str_bytes + strb_bytes + [0,2] + [0,0,0,1] + [LogType.ChangeContextFloatU32.value]))
            self.assertEqual(ChangeContextListU16(1, ["a", "b"], [[5,5],[256, 1]]).get_log_bytes(), bytearray([0,0,0,5,0,0,0,5,2] + [0,0,1,0,0,0,0,1,2] + str_bytes + strb_bytes + [0,2] + [0,0,0,1] + [LogType.ChangeContextListU16.value]))
            self.assertEqual(ChangeContextNoArgs(1, ["a"]).get_log_bytes(), bytearray(str_bytes + [0,1] + [0,0,0,1] + [LogType.ChangeContextNoArgs.value]))
            self.assertEqual(FinishedWriting().get_log_bytes(), bytearray([LogType.FinishedWriting.value]))
            self.assertEqual(SentBatch().get_log_bytes(), bytearray([LogType.SentBatch.value]))
            self.assertEqual(AckedBatch().get_log_bytes(), bytearray([LogType.AckedBatch.value]))
            self.assertEqual(SentFinalResult("a", 1).get_log_bytes(), bytearray(str_bytes + [0,0,0,1] + [LogType.SentFinalResult.value]))
            self.assertEqual(FinishedSendingResultsOfClient(65536).get_log_bytes(), bytearray([0,1,0,0] + [LogType.FinishedSendingResultsOfClient.value]))
            self.assertEqual(FinishedClient().get_log_bytes(), bytearray([LogType.FinishedClient.value]))
            self.assertEqual(ChangeMetadata(["a"],[2]).get_log_bytes(), bytearray([0,0,0,2] + str_bytes + [0,1] + [LogType.ChangedMetadata.value]))

        def test_write_logs(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            logger.log(ChangeContextFloat(1, ["a"],[1.1]))
            logger.log(ChangeContextFloatU32(1, ["a","b"], [1.1, 2.2], [1,2]))
            logger.log(ChangeContextListU16(1, ["a", "b"], [[5,5],[256, 1]]))
            logger.log(ChangeContextNoArgs(1, ["a"]))
            logger.log(FinishedWriting())
            logger.log(SentBatch())
            logger.log(AckedBatch())
            logger.log(SentFinalResult("a", 1))
            logger.log(FinishedSendingResultsOfClient(65536))
            logger.log(FinishedClient())
            logger.log(ChangeMetadata(["a"],[2]))
            mock_file.seek(0)
            self.assertEqual(mock_file.read(1000), self.get_mock_file().read(1000))

        def test_read_empty_log_file(self):
            mock_file = BytesIO(b"")
            logger = LogReadWriter(mock_file)
            self.assertEqual(logger.read_last_log(), None)

        def test_read_log_file(self):
            mock_file = self.get_mock_file()
            logger = LogReadWriter(mock_file)
            self.assertEqual(ChangeMetadata(["a"],[2]), logger.read_last_log())
            self.assertEqual(FinishedClient(), logger.read_curr_log())
            self.assertEqual(FinishedSendingResultsOfClient(65536),logger.read_curr_log())
            self.assertEqual(SentFinalResult("a", 1), logger.read_curr_log())
            self.assertEqual(AckedBatch(), logger.read_curr_log())
            self.assertEqual(SentBatch(), logger.read_curr_log())
            self.assertEqual(FinishedWriting(), logger.read_curr_log())
            self.assertEqual(ChangeContextNoArgs(1, ["a"]), logger.read_curr_log())
            self.assertEqual(ChangeContextListU16(1, ["a", "b"], [[5,5],[256, 1]]), logger.read_curr_log())
            self.assertEqual(ChangeContextFloatU32(1, ["a", "b"], [1.1, 2.2], [1,2]), logger.read_curr_log())
            self.assertEqual(ChangeContextFloat(1, ["a"], [1.1]), logger.read_curr_log())
            self.assertAlmostEqual(None, logger.read_curr_log())

        def test_read_until(self):
            mock_file = self.get_mock_file()
            logger = LogReadWriter(mock_file)

            logs = logger.read_until_log_type(LogType.FinishedSendingResultsOfClient)
            self.assertEqual(logs, [ChangeMetadata(["a"],[2]), FinishedClient(), FinishedSendingResultsOfClient(65536)])
        
        def test_read_until_but_log_not_in_file(self):
            file = BytesIO(b"")
            logger = LogReadWriter(file)
            logger.log(FinishedClient())
            logger.log(SentBatch())
            logs = logger.read_until_log_type(LogType.AckedBatch)
            self.assertEqual(logs, [SentBatch(), FinishedClient()])

    unittest.main()