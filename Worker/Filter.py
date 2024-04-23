from Worker import Worker

class BookFilter(Worker):
    def __init__(self, condicion, message_type):
        self.attribute = condicion
        self.message_class = message_type

    def process_message(self, batch):
        amount_of_messages = batch[0]
        batch = batch[1:]
        filtered_messages = []
        for _ in range(amount_of_messages):
            message, bytes_used = self.message_class.from_bytes(batch)
            batch = batch[bytes_used:]
            filtered_message = message.filter()
            if filtered_message != None:
                filtered_messages.append(filtered_message)
        
        return filtered_messages
    
class BookReviewFilter(Worker):
    def __init__(self, condicion, message_type):
        return