import signal

from utils.Batch import AMOUNT_OF_CLIENT_ID_BYTES, Batch, SeqNumGenerator
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE
from utils.DatasetHandler import DatasetReader
from utils.SenderID import SenderID
from utils.auxiliar_functions import recv_exactly, send_all

CLIENTS_SENDER_ID = SenderID(0,0,0)
NO_CLIENT_ID = 2**(8*AMOUNT_OF_CLIENT_ID_BYTES) - 1

class ClientReader():
    def __init__(self, id, socket, book_reader:DatasetReader, review_reader:DatasetReader, batch_size):
        self.socket = socket
        self.book_reader = book_reader
        self.review_reader = review_reader
        self.batch_size = batch_size
        self.id = id
        self.finished = False

    def start(self, book_reading_pos, review_reading_pos):
        if book_reading_pos != None:
            book_reading_pos = self.send_all_from_dataset(BOOK_MSG_TYPE, self.book_reader, book_reading_pos)
        if review_reading_pos != None:
            review_reading_pos = self.send_all_from_dataset(REVIEW_MSG_TYPE, self.review_reader, review_reading_pos)
        
        sent_eof = self.send_eof(book_reading_pos == None and review_reading_pos == None)
            
        self.close()
        if sent_eof:
            return None, None
        SeqNumGenerator.seq_num -= 1
        return book_reading_pos, review_reading_pos

    def send_batch(self, batch):
        try:
            send_all(self.socket, batch.to_bytes())
            if not self.receive_ack():
                SeqNumGenerator.seq_num -= 1
                return False
        except OSError as e:
            print(f"[ClientReader] Socket disconnected")
            SeqNumGenerator.seq_num -= 1
            return False
        return True

    def receive_ack(self):
        if recv_exactly(self.socket, 1) == None:
            print(f"[ClientReader] Socket disconnected")
            return False
        return True

    def send_all_from_dataset(self, object_type, reader, already_sent):
        reader.skip_to(already_sent)
        while True:
            datasetLines = reader.read_lines(self.batch_size, object_type)
            if len(datasetLines) == 0:
                break
            batch = Batch.new(self.id, CLIENTS_SENDER_ID, datasetLines)
            if not self.send_batch(batch):
                break
            
            already_sent = reader.file.tell()
        return already_sent
    
    def send_eof(self, already_sent=False):
        send_id = self.id
        if already_sent:
            send_id = NO_CLIENT_ID
        return self.send_batch(Batch.eof(send_id, CLIENTS_SENDER_ID))

    def close(self):
        self.socket.close()
        self.book_reader.close()
        self.review_reader.close()