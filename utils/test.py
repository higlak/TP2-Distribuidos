class DatasetLine():
    def __init__(self, string, object_type):
        self.datasetLine = string
        self.datasetLineType = object_type
        
    def to_bytes(self):
        byte_array = integer_to_big_endian_byte_array(self.datasetLineType, 1)
        datasetlineencoded = self.datasetLine.encode()
        byte_array.extend(integer_to_big_endian_byte_array(len(datasetlineencoded), 2))
        byte_array.extend(byte_array)
        return byte_array
    
def integer_to_big_endian_byte_array(number, amount_of_bytes):
    byte_array = bytearray()
    if number != None:
        for i in range(amount_of_bytes):
            byte = (number >> (8 * (amount_of_bytes-i-1))) & 0xff
            byte_array.append(byte)
    return byte_array

def remove_bytes(array, finish, start=0):
    elements = array[start:finish]
    del array[start:finish]
    return elements

string = "Mensa Number Puzzles (Mensa Word Games for Kids),\"Acclaimed teacher and puzzler Evelyn B. Christensen has created one hundred brand-new perplexing and adorably illustrated games for young puzzlers. There is something for every type of learner here, including number puzzles, word puzzles, logic puzzles, and visual puzzles. She has also included secret clues the solver can consult if they need a hint, making the puzzles even more flexible for a wide skill range of puzzle-solvers. Arranged from easy to difficult, this is a great book for any beginning puzzler. With the game types intermixed throughout, it’s easy for a child who thinks they like only math or only word puzzles to stumble across a different kind of puzzle, get hooked, and discover—oh, they like that kind, too! Regularly practicing a variety of brain games can help improve and develop memory, concentration, creativity, reasoning, and problem-solving skills. Mensa’s® Fun Puzzle Challenges for Kids is a learning tool everyone will enjoy!\",['Evelyn B. Christensen'],http://books.google.com/books/content?id=tX1IswEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api,http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&cd=1&source=gbs_api,Sky Pony,2018-11-06,http://books.google.nl/books?id=tX1IswEACAAJ&dq=Mensa+Number+Puzzles+(Mensa+Word+Games+for+Kids)&hl=&source=gbs_api,['Juvenile Nonfiction'],"
print("Len string: ", len(string))
datasetline = DatasetLine(string, 0)
x = datasetline.to_bytes()
x.decode()

def byte_array_to_big_endian_integer(bytes):
    number = 0
    for i in range(0, len(bytes)):
        number = number | bytes[i] << 8*(len(bytes)-1-i)
    return number

print(byte_array_to_big_endian_integer(x[1:3]))