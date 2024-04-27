from time import sleep
from CommunicationMiddleware.middleware import Communicator
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.Batch import Batch

GATEWAY_EXCHANGE_NAME = 'GATEWAY_EXCHANGE'

def messages_for_query1():
    messages = []
    for i in range(30):
        msg = QueryMessage(BOOK_MSG_TYPE, title=str(i), year=1995+i)
        if i%2:
            msg.categories = ['fiction']  
        messages.append(msg)
    return messages

def messages_for_query2():
    messages = []
    for i in range(30):
        msg = QueryMessage(BOOK_MSG_TYPE, title=str(i), year=1950+5*i)
        if i%2:
            msg.authors = ['Author1']  
        messages.append(msg)
    return messages

def main():
    com = Communicator()
    messages = messages_for_query1()
    #messages = messages_for_query2()
    sleep(10)
    print("Sending Batch")
    com.produce_message('1.0', Batch(messages).to_bytes())
    batch_bytes = com.consume_message(GATEWAY_EXCHANGE_NAME)
        
    batch = Batch.from_bytes(batch_bytes)
    print("Recibi de resultado eto: ", batch.messages[0].title)
    sleep(10)
main()