def byte_array_to_big_endian_integer(bytes):
    number = 0
    for i in range(0, len(bytes)):
        number = number | bytes[i] << 8*(len(bytes)-1-i)
    return number

def integer_to_big_endian_byte_array(number, amount_of_bytes):
    byte_array = bytearray()
    for i in range(amount_of_bytes):
        byte = (number >> (8 * (amount_of_bytes-i-1))) & 0xff
        byte_array.append(byte)
    return byte_array