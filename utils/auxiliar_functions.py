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

def length(iterable):
    """
    Receives and iterable and returns its length
    If fails, returns None
    """
    try:
        return len(iterable)
    except:
        return None
        
def remove_bytes(array, finish, start=0):
    elements = array[start:finish]
    del array[start:finish]
    return elements