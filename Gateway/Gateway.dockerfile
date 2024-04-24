FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./CommunicationMiddleware /root/CommunicationMiddleware
COPY ./utils /root/utils
COPY ./Gateway/Gateway.py /root/main.py
COPY ./__init__.py /root/__init__.py
ENV PYTHONPATH "${PYTHONPATH}:/root"

CMD python3 /root/main.py