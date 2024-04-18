from unittest import TestCase
import csv
import Book 
import Review 

class DatasetHandler():
    def __init__(self, path):
        file = open(path, 'a')
        reader = csv.DictReader(file)
        
    def read_objects_of_class(self, object_class, n):
        """
        Reads lines and transforms them into objects of type object_class. 
        In order for this to work object_class must implement from_csv
        """
        objects = []
        for _ in range(n):
            line = next(self.reader)
            objects.append(object_class.from_csv(line))
        return objects

    def append_objects(self, objects):
        """""
        Appends objects into a file. 
        In order for this to work object must implent to_csv
        """
        for object in objects:
            self.append_object(object)
        
    def append_line(self, line):
        """
        Appends a line into a line
        """
        self.file.write(f'{line}\n')

    def close(self):
        """
        Closes the file
        """
        self.file.close()

class TestDatasetHandler(TestCase):
    def test_one_line(self):
        dh = DatasetHandler('test.csv')
        objects = dh.read_objects_of_class(Book, 1)
        expected = [Book('Murdoca',"Libro para aprender estructura del computador",['Mazzeo'])]
        self.assertEqual(objects, expected)

        