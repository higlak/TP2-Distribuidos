from time import sleep
from Workers.Filters import CategoryFilter
from utils.Message import CATEGORIES_FIELD

sleep(15)
def main():
    worker = CategoryFilter('fiction', [CATEGORIES_FIELD])
    worker.start()
    print("hola")

main()