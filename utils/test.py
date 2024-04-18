import csv

with open('./test.csv') as archivo:
    lector = csv.DictReader(archivo)
    linea = next(lector)
    print(linea)