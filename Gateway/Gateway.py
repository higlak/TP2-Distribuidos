from time import sleep
from CommunicationMiddleware.middleware import Communicator
from utils.Message import Message, BOOK_MSG_TYPE
from utils.Batch import Batch

sleep(16)
def main():
    com = Communicator(routing_keys=['1'])
    messages = []
    for i in range(10):
        msg = Message(BOOK_MSG_TYPE, title=str(i), year=1995+i)
        if i%2:
            msg.categories = ['fiction']  
        messages.append(msg)
    print("Sending Batch :", Batch(messages).to_bytes())
    com.publish_message_next_routing_key('1.1' ,Batch(messages).to_bytes())
    print("Eof")
    com.publish_message_next_routing_key('1.1' ,Batch([]).to_bytes())

main()