FROM rabbitmq:latest
# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./utils /root/utils
COPY ./Client/Client.py /root/main.py
COPY ./Client/ClientReadWriter /root/ClientReadWriter

ENTRYPOINT ["python3", "/root/main.py", "/data/book_test.csv", "/data/review_test.csv"]
