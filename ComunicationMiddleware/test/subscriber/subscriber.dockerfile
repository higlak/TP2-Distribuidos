FROM rabbitmq:latest

# Instalar Python 3
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY subscriber.py /root/subscriber.py
CMD /root/subscriber.py 