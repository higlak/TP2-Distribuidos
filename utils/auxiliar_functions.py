import os

QUERY_SEPARATOR = ','

# Receives bytes until n bytes have been received. If cannot receive n bytes None is returned
def recv_exactly(socket, n):
    buffer = bytes()
    while n > 0:
        received = socket.recv(n)
        if len(received) == 0:
            return None
        buffer += received
        n -= len(received)
    return buffer 

# Sends bytes until all of them are sent, or a failure occurs
def send_all(socket, byte_array):
    while len(byte_array) > 0:
        sent = socket.send(byte_array)
        byte_array = byte_array[sent:]
    return

def byte_array_to_big_endian_integer(bytes):
    number = 0
    for i in range(0, len(bytes)):
        number = number | bytes[i] << 8*(len(bytes)-1-i)
    return number

def integer_to_big_endian_byte_array(number, amount_of_bytes):
    byte_array = bytearray()
    if number != None:
        for i in range(amount_of_bytes):
            byte = (number >> (8 * (amount_of_bytes-i-1))) & 0xff
            byte_array.append(byte)
    return byte_array

def get_env_list(param):
    l = os.getenv(param).split(QUERY_SEPARATOR)
    if l == [""]:
        return []
    return l

def length(iterable):
    """
    Receives and iterable and returns its length
    If fails, returns None
    """
    try:
        return len(iterable)
    except:
        return None
    
def append_extend(l, element_or_list):
    if isinstance(element_or_list, list):
        l.extend(element_or_list)
    else:
        l.append(element_or_list)
        
def remove_bytes(array, finish, start=0):
    elements = array[start:finish]
    del array[start:finish]
    return elements

def encode(string :str):
    if string == None:
        return None
    return string.encode()