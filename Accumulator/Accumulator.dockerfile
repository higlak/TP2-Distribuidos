FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika
RUN apt-get update && apt-get install -y python3-pip
RUN pip install textblob

COPY ./CommunicationMiddleware /root/CommunicationMiddleware
COPY ./Workers /root/Workers
COPY ./utils /root/utils
COPY ./Accumulator/Accumulator.py /root/main.py
RUN chmod +x /root/main.py

CMD python3 /root/main.py