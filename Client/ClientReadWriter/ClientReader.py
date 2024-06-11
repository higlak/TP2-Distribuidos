import signal

from utils.Batch import Batch
from utils.QueryMessage import BOOK_MSG_TYPE, REVIEW_MSG_TYPE
from utils.DatasetHandler import DatasetReader
from utils.SenderID import SenderID

CLIENTS_SENDER_ID = SenderID(0,0,0)

class ClientReader():
    def __init__(self, id, socket, book_reader:DatasetReader, review_reader:DatasetReader, batch_size):
        self.socket = socket
        self.book_reader = book_reader
        self.review_reader = review_reader
        self.batch_size = batch_size
        self.id = id

    def handle_SIGTERM(self, _signum, _frame):
        print("\n\n [ClientReader] SIGTERM detected \n\n")
        self.socket.close()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_SIGTERM)
        try: 
            self.send_all_from_dataset(BOOK_MSG_TYPE, self.book_reader)
            self.send_all_from_dataset(REVIEW_MSG_TYPE, self.review_reader)
            self.send_eof()
        except OSError:
            print(f"[ClientReader] Socket disconnected")
        self.close_readers()

    def send_all_from_dataset(self, object_type, reader):
        while True:
            datasetLines = reader.read_lines(self.batch_size, object_type)
            if len(datasetLines) == 0:
                return True
            batch = Batch.new(self.id, CLIENTS_SENDER_ID, datasetLines)
            self.socket.send(batch.to_bytes())
    
    def send_eof(self):
        self.socket.send(Batch.eof(self.id, CLIENTS_SENDER_ID).to_bytes())

    def close_readers(self):
        self.book_reader.close()
        self.review_reader.close()