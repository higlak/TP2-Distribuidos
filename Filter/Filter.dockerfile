FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./CommunicationMiddleware /root/CommunicationMiddleware
COPY ./Workers /root/Workers
COPY ./utils /root/utils
COPY ./Filter/Filter.py /root/main.py
RUN chmod +x /root/main.py

ENTRYPOINT ["python3", "/root/main.py"]