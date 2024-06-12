from unittest import TestCase
import csv
from utils.auxiliar_functions import integer_to_big_endian_byte_array, byte_array_to_big_endian_integer, remove_bytes, recv_exactly
from utils.QueryMessage import MSG_TYPE_BYTES, BOOK_MSG_TYPE

DATASET_LINE_LEN_BYTES = 2

class DatasetLine():
    def __init__(self, string, object_type):
        self.datasetLine = string
        self.datasetLineType = object_type
        
    def to_bytes(self):
        byte_array = integer_to_big_endian_byte_array(self.datasetLineType, MSG_TYPE_BYTES)
        encoded_datasetLine = self.datasetLine.encode()
        byte_array.extend(integer_to_big_endian_byte_array(len(encoded_datasetLine), DATASET_LINE_LEN_BYTES))
        byte_array.extend(encoded_datasetLine)
        return byte_array

    def __len__(self):
        return len(self.datasetLine)

    def __repr__(self) -> str:
        return str(self.datasetLineType) + ' ' + self.datasetLine

    @classmethod
    def from_bytes(cls, byte_array):
        msg_type = byte_array_to_big_endian_integer(remove_bytes(byte_array, MSG_TYPE_BYTES))
        length = byte_array_to_big_endian_integer(remove_bytes(byte_array, DATASET_LINE_LEN_BYTES))
        string = remove_bytes(byte_array, length).decode()
        return DatasetLine(string, msg_type)
    
    @classmethod
    def from_socket(cls, sock):
        byte_array = recv_exactly(sock, MSG_TYPE_BYTES + DATASET_LINE_LEN_BYTES)
        if not byte_array:
            return None
        datasetLineType = byte_array[0]
        datasetLineLen = byte_array_to_big_endian_integer(byte_array[1:])
        datasetLine_bytes = recv_exactly(sock, datasetLineLen)
        if not datasetLine_bytes:
            return None
        return DatasetLine(datasetLine_bytes.decode(), datasetLineType)
    
    def is_book(self):
        return self.datasetLineType == BOOK_MSG_TYPE
        

class DatasetReader():
    def __init__(self, path):
        try:
            self.file = open(path, 'r', encoding='Utf-8')
            self.file.readline()
        except:
            print(f"Unable to open path {path}")
            return None

    def read_lines(self, n, object_type):
        lines = []
        for _ in range(n):
            line  = self.file.readline().rstrip("\n")
            if line == '':
                break
            lines.append(DatasetLine(line, object_type))
        return lines

    def close(self):
        """
        Closes the file
        """
        self.file.close()

class DatasetWriter():
    def __init__(self, path, columns):
        self.file = open(path, 'a', encoding='Utf-8')
        self.file.seek(0)
        self.file.truncate()
        self.header = False
        self.writer = csv.DictWriter(self.file, fieldnames=columns, lineterminator='\n')

    def append_objects(self, objects):
        """""
        Appends objects into a file. 
        In order for this to work object must implent to_csv
        """
        for object in objects:
            self.append_object(object)

    def append_object(self, object):
        """
        Appends an object using the get_csv_values method, which must ruturn
        the amount of values specified when the Writer was created
        """
        if not self.header:
            self.writer.writeheader()
            self.header = True
        columns = self.writer.fieldnames
        values = object.get_csv_values()
        line = {}
        for i in range(len(columns)):
            try:
                line[columns[i]] = values[i]
            except:
                print(columns)
                print(values)
            line[columns[i]] = values[i]

        self.writer.writerow(line)

    def close(self):
        """
        Closes the file
        """
        self.file.close()

if __name__ == '__main__':
    import unittest
    import time
    from utils.Book import Book 
    from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE

    class TestDatasetReader(TestCase):
        def test_line(self):
            return "Its Only Art If Its Well Hung!,,['Julie Strain'],http://books.google.com/books/content?id=DykPAAAACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api,http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&cd=1&source=gbs_api,,1996,http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&source=gbs_api,['Comics & Graphic Novels'],"

        def test_read_lines(self):
            dh = DatasetReader('./utils/test.csv')
            line = dh.read_lines(1, BOOK_MSG_TYPE)[0]
            expected = DatasetLine(self.test_line(), BOOK_MSG_TYPE)
            print(line)
            print(expected)
            self.assertEqual(line.to_bytes(), expected.to_bytes())
        
        def test_datasetLine_from_bytes(self):
            line = DatasetLine(self.test_line(), BOOK_MSG_TYPE)
            line_bytes = line.to_bytes()
            self.assertEqual(line.to_bytes(), DatasetLine.from_bytes(line_bytes).to_bytes())

    class TestDatasetWriter(TestCase):
        def test_write_query_result(self):
            columns = ["Title", "author"]
            dw = DatasetWriter('./utils/test_result.csv', columns)
            result = QueryMessage(BOOK_MSG_TYPE, title="Murdoca", authors="['Mazzeo']")
            dw.append_object(result)
            dw.close()

            file = open('./utils/test_result.csv', 'r', encoding='Utf-8',)
            reader = csv.DictReader(file)
            values = list(next(reader).values())
            file.close()
            self.assertEqual(values, result.get_csv_values())
        
        def test_write_query_result(self):
            columns = ["Title", "author"]
            dw = DatasetWriter('test_result.csv', columns)
            result1 = QueryMessage(BOOK_MSG_TYPE, title="Murdoca", authors="['Mazzeo']")
            result2 = QueryMessage(BOOK_MSG_TYPE, title="Fisica", authors="['Sears', 'Semanski']")
            dw.append_objects([result1, result2])
            dw.close()

            file = open('test_result.csv', 'r', encoding='Utf-8',)
            reader = csv.DictReader(file)
            values = [list(next(reader).values()), list(next(reader).values())]
            file.close()
            self.assertEqual(values, [result1.get_csv_values(), result2.get_csv_values()])
            
    unittest.main()