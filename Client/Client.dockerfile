FROM rabbitmq:latest
# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

COPY ./utils /root/utils
COPY ./Client/Client.py /root/main.py
#COPY ./data/test.csv /root/book.csv
#COPY ./data/test.csv /root/review.csv
#COPY ./__init__.py /root/__init__.py
#ENV PYTHONPATH "${PYTHONPATH}:/root"

CMD python3 /root/main.py /data/books_data.csv /data/test.csv