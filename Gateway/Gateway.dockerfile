FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./CommunicationMiddleware /root/CommunicationMiddleware
COPY ./utils /root/utils
COPY ./Gateway/GatewayInOut /root/GatewayInOut
COPY ./Persistance /root/Persistance
COPY ./Gateway/Gateway.py /root/main.py

ENTRYPOINT ["python3", "/root/main.py"]