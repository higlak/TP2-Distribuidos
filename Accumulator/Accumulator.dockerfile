FROM rabbitmq:latest

# Instalar Python 3 y pika
RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-pika

ARG TEXTBLOB
ENV TEXTBLOB=${TEXTBLOB}

COPY ./Accumulator/textblob.sh /root/textblob.sh
RUN chmod +x /root/textblob.sh
RUN /root/textblob.sh

#RUN apt-get update && apt-get install -y python3-pip && pip install textblob

COPY ./CommunicationMiddleware /root/CommunicationMiddleware
COPY ./Workers /root/Workers
COPY ./utils /root/utils
COPY ./Accumulator/Accumulator.py /root/main.py
RUN chmod +x /root/main.py

ENTRYPOINT ["python3", "/root/main.py"]