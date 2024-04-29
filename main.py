import csv
from utils.DatasetHandler import *
from utils.Book import *
import pprint


reader = DatasetReader("/home/palito/Downloads/archive/books_data.csv")
a = reader.read_objects_of_class(Book, 3)[1]
pprint.pprint(a)
# # Supongamos que tienes una línea de CSV en una cadena llamada 'linea_csv'
# linea_csv = 'valor1,"un,unico,valor",valor2'

# # Creamos un lector de CSV utilizando un iterable que contenga la línea como un solo elemento
# lector_csv = csv.DictReader([linea_csv, '1,1,1'])

# # Leemos la primera (y en este caso, única) línea del CSV
# valores = next(lector_csv)
# #print(valores)

