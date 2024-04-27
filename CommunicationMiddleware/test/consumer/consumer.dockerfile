FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY test/consumer/consumer.py /root/consumer.py
COPY ../../middleware.py /root/middleware.py

CMD /root/consumer.py > /out.txt