FROM rabbitmq:latest

RUN apt-get update && apt-get install -y python3

COPY ./Waker/main.py /root/main.py
COPY ./Waker/Waker.py /root/Waker.py
COPY ./utils /root/utils

ENTRYPOINT ["python3", "/root/main.py"]