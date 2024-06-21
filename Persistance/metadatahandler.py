from Persistance.KeyValueStorage import KeyValueStorage
from Persistance.log import ChangingFile
from utils.Batch import SeqNumGenerator
from utils.SenderID import SenderID


METADATA_KEY_BYTES = 25 + 18
METADATA_NUM_BYTES = 4
METADATA_FILENAME = 'metadata.bin'
LAST_SENT_SEQ_NUM = "last sent seq_num"
CLIENT_PENDING_EOF = "pending eof client"
LAST_RECEIVED_FROM_WORKER = "last received from worker" 

class MetadataHandler():
    def __init__(self, storage, logger):
        self.storage = storage
        self.logger = logger
    
    @classmethod
    def new(cls, directory, logger):
        storage, previouse_metadata = KeyValueStorage.new(
            directory + METADATA_FILENAME, str, METADATA_KEY_BYTES, [int], [METADATA_NUM_BYTES])
        if not storage or previouse_metadata == None:
            return None
        return MetadataHandler(storage, logger)
    
    def load_stored_metadata(self):
        stored_metadata = self.storage.get_all_entries()
        SeqNumGenerator.set_seq_num(stored_metadata.pop(LAST_SENT_SEQ_NUM, None))
        pending_eof = {}
        last_received_batch = {}

        while len(stored_metadata) > 0:
            entry = stored_metadata.popitem()
            if entry[0].startswith(CLIENT_PENDING_EOF):
                client_id = int(entry[0].strip(CLIENT_PENDING_EOF))
                pending_eof[client_id] = entry[1]
            elif entry[0].startswith(LAST_RECEIVED_FROM_WORKER):
                sender_id = SenderID.from_string(entry[0].strip(LAST_RECEIVED_FROM_WORKER))
                last_received_batch[sender_id] = entry[1]
        return pending_eof, last_received_batch
    
    def remove_client(self, client_id):
        self.storage.remove(CLIENT_PENDING_EOF+str(client_id))

    def update_seq_num(self):
        self.storage.store(LAST_SENT_SEQ_NUM, [SeqNumGenerator.seq_num])

    def dump_metadata_to_disk(self, last_received_batch, pending_eof, received_batch=None):
        keys = []
        old_entries = []
        new_entries = []

        keys.append(LAST_SENT_SEQ_NUM)
        if SeqNumGenerator.seq_num - 1 < 0:
            old_entries.append(None)
        else:
            old_entries.append([SeqNumGenerator.get_log_seq_num()])
        new_entries.append([SeqNumGenerator.seq_num])

        if received_batch:
            keys.append(LAST_RECEIVED_FROM_WORKER + str(received_batch.sender_id))
            old_batch_seq_num = last_received_batch.get(received_batch.sender_id, None)
            if old_batch_seq_num != None:
                old_batch_seq_num = [old_batch_seq_num]
            old_entries.append(old_batch_seq_num)
            new_entries.append([received_batch.seq_num])

            if received_batch.is_empty():
                keys.append(CLIENT_PENDING_EOF + str(received_batch.client_id))
                old_entries.append([pending_eof[received_batch.client_id] + 1])
                new_entries.append([pending_eof[received_batch.client_id]])

        if len(keys) > 0:
            self.logger.log(ChangingFile(METADATA_FILENAME, keys, old_entries))
            self.storage.store_all(keys, new_entries)