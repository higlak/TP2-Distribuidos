FROM rabbitmq:latest
# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./utils /root/utils
COPY ./Client/Client.py /root/main.py

ENTRYPOINT ["python3", "/root/main.py", "/data/books_data.csv", "/data/Books_rating.csv"]
