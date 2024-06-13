FROM rabbitmq:latest

RUN apt-get update && apt-get install -y python3

COPY ./Waker/Waker.py /root/main.py
RUN chmod +x /root/main.py

ENTRYPOINT ["python3", "/root/main.py"]