from Workers.Filters import *
import os 
print("Directory", os.getcwd())
def main():
    w = CategoryFilter('hola', ['categories'])
    w.start()

main()