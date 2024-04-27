from unittest import TestCase
import csv
import pprint

class DatasetReader():
    def __init__(self, path):
        try:
            self.file = open(path, 'r', encoding='Utf-8')
        except:
            print(f"Unable to open path {path}")
            return None
        self.reader = csv.DictReader(self.file)

    def read_lines(self, n):
        lines = []
        for i in range(n):
            try: 
                line = next(self.reader)
                lines.append(line)
            except StopIteration:
                break
        return lines

    def read_objects_of_class(self, object_class, n):
        """
        Reads lines and transforms them into objects of type object_class. 
        In order for this to work object_class must implement from_csv
        """
        objects = []
        for _ in range(n):
            try: 
                attributes = next(self.reader)
                objects.append(object_class.from_csv(attributes))
            except StopIteration:
                break
        return objects

    def close(self):
        """
        Closes the file
        """
        self.file.close()

class DatasetWriter():
    def __init__(self, path, columns):
        self.file = open(path, 'a', encoding='Utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=columns, lineterminator='\n')
        self.writer.writeheader()

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
        columns = self.writer.fieldnames
        values = object.get_csv_values()
        line = {}
        for i in range(len(columns)):
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
    from Book import Book 
    from QueryResult import QueryResult

    class TestDatasetReader(TestCase):
        def test_book1(self):
            return Book('Its Only Art If Its Well Hung!', 
                            '', 
                            ['Julie Strain'], 
                            'http://books.google.com/books/content?id=DykPAAAACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api', 
                            'http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&cd=1&source=gbs_api', 
                            '', 
                            '1996', 
                            'http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&source=gbs_api', 
                            ['Comics & Graphic Novels'], 
                            None)
        
        def test_book2(self):
            return Book('Dr. Seuss: American Icon',
                        "Philip Nel takes a fascinating look into the key aspects of Seuss's career - his poetry, politics, art, marketing, and place in the popular imagination.\" \"Nel argues convincingly that Dr. Seuss is one of the most influential poets in America. His nonsense verse, like that of Lewis Carroll and Edward Lear, has changed language itself, giving us new words like \"nerd.\" And Seuss's famously loopy artistic style - what Nel terms an \"energetic cartoon surrealism\" - has been equally important, inspiring artists like filmmaker Tim Burton and illustrator Lane Smith. --from back cover",
                        ['Philip Nel'],
                        'http://books.google.com/books/content?id=IjvHQsCn_pgC&printsec=frontcover&img=1&zoom=1&edge=curl&source=gbs_api',
                        'http://books.google.nl/books?id=IjvHQsCn_pgC&printsec=frontcover&dq=Dr.+Seuss:+American+Icon&hl=&cd=1&source=gbs_api',
                        'A&C Black',
                        '2005-01-01',
                        'http://books.google.nl/books?id=IjvHQsCn_pgC&dq=Dr.+Seuss:+American+Icon&hl=&source=gbs_api',
                        ['Biography & Autobiography'],
                        None)

        def test_books(self):
            return [self.test_book1(), self.test_book2()]
        
        def test_read_one_object_of_class(self):
            dh = DatasetReader('test.csv')
            objects = dh.read_objects_of_class(Book, 1)
            expected = [self.test_book1()]
            dh.close()
            self.assertEqual(objects, expected)

        def test_read_multiple_objects_of_class(self):
            dh = DatasetReader('test.csv')
            objects = dh.read_objects_of_class(Book, 2)
            expected = self.test_books()       
            dh.close()
            self.assertEqual(objects, expected)
            
        def test_read_more_than_available_object_of_class(self):
            dh = DatasetReader('test.csv')
            objects = dh.read_objects_of_class(Book, 3)
            expected = self.test_books()
            dh.close()
            self.assertEqual(objects, expected)
    
    class TestDatasetWriter(TestCase):
        def test_write_query_result(self):
            columns = ["Title", "author"]
            dw = DatasetWriter('test_result.csv', columns)
            result = QueryResult("Murdoca", "['Mazzeo']")
            dw.append_object(result)
            dw.close()

            file = open('test_result.csv', 'r', encoding='Utf-8',)
            reader = csv.DictReader(file)
            values = list(next(reader).values())
            file.close()
            self.assertEqual(values, result.get_csv_values())
        
        def test_write_query_result(self):
            columns = ["Title", "author"]
            dw = DatasetWriter('test_result.csv', columns)
            result1 = QueryResult("Murdoca", "['Mazzeo']")
            result2 = QueryResult("Fisica", "['Sears', 'Semanski']")
            dw.append_objects([result1, result2])
            dw.close()

            file = open('test_result.csv', 'r', encoding='Utf-8',)
            reader = csv.DictReader(file)
            values = [list(next(reader).values()), list(next(reader).values())]
            file.close()
            self.assertEqual(values, [result1.get_csv_values(), result2.get_csv_values()])
            
    unittest.main()