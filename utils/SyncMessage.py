from utils.auxiliar_functions import remove_bytes, byte_array_to_big_endian_integer, integer_to_big_endian_byte_array

SYNC_MSG_TYPE = 0
SYNC_DONE_MSG_TYPE = 1

class SyncMessage():
    def __init__(self, msg_type, worker_up_id):
        self.msg_type = msg_type
        self.worker_up_id = worker_up_id

    @classmethod
    def from_bytes(byte_array):
        msg_type = remove_bytes(byte_array, 1)[0]
        worker_up_id = byte_array_to_big_endian_integer((byte_array, 2))
        return SyncMessage(msg_type, worker_up_id)

    def to_bytes(self):
        byte_array = bytearray([self.msg_type])
        byte_array.append(integer_to_big_endian_byte_array(self.parameters_to_bytes()))
       
        return byte_array
    
    def is_sync_message():
        return True