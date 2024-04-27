from time import sleep
import time
from CommunicationMiddleware.middleware import Communicator
from utils.QueryMessage import QueryMessage, BOOK_MSG_TYPE
from utils.Batch import Batch
from utils.auxiliar_functions import recv_exactly, send_all, byte_array_to_big_endian_integer
from utils.DatasetHandler import DatasetLine
import socket

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

def start_listening():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('',12345))
    server_socket.listen()
    client_connection, addr = server_socket.accept()
    print(f"\n\n Cliente conectado {addr}\n\n")
    return server_socket, client_connection, addr

def recv_dataset_line_batch(client_connection):
    ammount_of_dataset_lines = recv_exactly(client_connection,1)
    print(ammount_of_dataset_lines)
    if ammount_of_dataset_lines == None:
        print("Fallo la lectura del socket")
        return None
    ammount_of_dataset_lines = ammount_of_dataset_lines[0]
    if ammount_of_dataset_lines == 0:
        #recibi un eof
        print("Recibi EOF")
        return None
    datasetlines =[]
    for i in range(ammount_of_dataset_lines):
        datasetline = DatasetLine.from_socket(client_connection)
        datasetlines.append(datasetline)
        print(datasetline)
    return datasetline


def main():
    #com = Communicator()
    server_socket, client_connection, addr = start_listening()
    while True:
        if not recv_dataset_line_batch(client_connection):
            break

    # messages = messages_for_query1()
    # #messages = messages_for_query2()
    # sleep(10)
    # print("Sending Batch")
    # com.produce_message('1.0', Batch(messages).to_bytes())
    # batch_bytes = com.consume_message(GATEWAY_EXCHANGE_NAME)
        
    # batch = Batch.from_bytes(batch_bytes)
    # print("Recibi de resultado eto: ", batch.messages[0].title)
    # sleep(10)
main()