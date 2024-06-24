from Persistance.KeyValueStorage import KeyValueStorage
from Persistance.log import ChangingFile
from utils.Batch import SeqNumGenerator
from utils.SenderID import SenderID


METADATA_KEY_BYTES = 25 + 18
METADATA_NUM_BYTES = 4
METADATA_FILENAME = 'metadata'
LAST_SENT_SEQ_NUM = "last sent seq_num"
CLIENT_PENDING_EOF = "pending eof client"
LAST_CLIENT_ID = "last clientid"
LAST_RECEIVED_FROM = "last received from" 

class MetadataHandler():
    def __init__(self, storage, logger, filename):
        self.storage = storage
        self.logger = logger
        self.filename = filename
        self.log_seq_num = -1
        self.log_last_client = -1
    
    @classmethod
    def new(cls, directory, logger, name_specifier=""):
        filename = METADATA_FILENAME + name_specifier + '.bin'
        storage = KeyValueStorage.new(
            directory + filename, str, METADATA_KEY_BYTES, [int], [METADATA_NUM_BYTES])
        if not storage:
            return None
        return MetadataHandler(storage, logger, filename)
    
    def load_stored_metadata(self, get_last_client=False):
        stored_metadata = self.storage.get_all_entries()

        self.last_sent_seq_num = stored_metadata.pop(LAST_SENT_SEQ_NUM, -1)
        self.log_last_client = stored_metadata.pop(LAST_CLIENT_ID, -1)
        pending_eof = {}
        last_received_batch = {}

        while len(stored_metadata) > 0:
            entry = stored_metadata.popitem()
            if entry[0].startswith(CLIENT_PENDING_EOF):
                client_id = int(entry[0].strip(CLIENT_PENDING_EOF))
                pending_eof[client_id] = entry[1]
            elif entry[0].startswith(LAST_RECEIVED_FROM):
                sender_id = SenderID.from_string(entry[0].strip(LAST_RECEIVED_FROM))
                last_received_batch[sender_id] = entry[1]
        
        if not get_last_client:
            return self.last_sent_seq_num , pending_eof, last_received_batch
        return self.last_sent_seq_num , pending_eof, last_received_batch, self.log_last_client
    
    def remove_client(self, client_id):
        self.storage.remove(CLIENT_PENDING_EOF+str(client_id))

    def update_seq_num(self):
        self.storage.store(LAST_SENT_SEQ_NUM, [SeqNumGenerator.seq_num])

    def dump_new_client(self, client_id, eof_to_receive):
        keys = [CLIENT_PENDING_EOF + str(client_id), LAST_CLIENT_ID]
        values = [[eof_to_receive], [client_id]]
        if self.log_last_client == -1:
            log_last_client = None
        else:
            log_last_client = [self.log_last_client]

        old_values = [None, log_last_client]

        self.logger.log(ChangingFile(self.filename, keys, old_values))
        self.storage.store_all(keys, values)

        self.log_last_client = client_id

    def dump_metadata_to_disk(self, last_received_batch, pending_eof, received_batch=None):
        keys = []
        old_entries = []
        new_entries = []

        if SeqNumGenerator.seq_num > self.log_seq_num:
            keys.append(LAST_SENT_SEQ_NUM)
            if self.log_seq_num == -1:
                old_entries.append(None)
            else:
                old_entries.append([self.log_seq_num])
            new_entries.append([SeqNumGenerator.seq_num])

        if received_batch:
            keys.append(LAST_RECEIVED_FROM + str(received_batch.sender_id))
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
            self.logger.log(ChangingFile(self.filename, keys, old_entries))
            self.log_seq_num = SeqNumGenerator.seq_num
            self.storage.store_all(keys, new_entries)

    def delete(self):
        self.storage.delete()
