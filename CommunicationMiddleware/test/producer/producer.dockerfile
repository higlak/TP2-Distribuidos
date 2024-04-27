FROM rabbitmq:latest

# Instalar Python 3
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY test/producer/producer.py /root/producer.py
COPY middleware.py /root/middleware.py

CMD /root/producer.py