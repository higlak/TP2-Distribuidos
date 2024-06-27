FROM ubuntu:20.04

RUN apt-get update && apt-get install -y python3
RUN apt-get update && apt-get install -y python3-docker
RUN apt-get update && apt-get install -y python3-distutils

COPY ./Waker/main.py /root/main.py
COPY ./Waker/Waker.py /root/Waker.py
COPY ./utils /root/utils

ENTRYPOINT ["python3", "/root/main.py"]