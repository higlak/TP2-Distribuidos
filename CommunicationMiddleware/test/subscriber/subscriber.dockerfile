FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY test/subscriber/subscriber.py /root/subscriber.py
COPY ../../middleware.py /root/middleware.py

CMD /root/subscriber.py > /out.txt