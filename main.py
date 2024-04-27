from Workers.Filters import *
import os 
print("Directory", os.getcwd())
def main():
    w = Filter('hola', ['categories'])
    w.start()

main()