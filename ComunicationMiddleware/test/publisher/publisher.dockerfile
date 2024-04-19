FROM rabbitmq:latest

# Instalar Python 3
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY publisher.py /root/publisher.py
CMD /root/publisher.py