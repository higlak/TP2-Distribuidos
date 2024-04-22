#!/bin/bash
publishers="1"
subscribers="2"

echo $0, $1, $2

while [[ $# -gt 0 ]]; do #cantidad de argumentos >= 0
    key="$1"
    case $key in
        --p)
            publishers="$2"
            shift 2       #move los argumentos 2 posiciones
            ;;
        --s)
            subscribers="$2"
            shift 2
            ;;
        *)
            echo "--p x --s son los args donde x es # de publishers y S # de subscribers"
            exit
            ;;
    esac
done

echo "Se levantaran $publishers producers y subscriber $subscribers"

rabbit_mq_text="version: '3'
services:
  rabbitmq:
    build:
      context: ./test/rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672"

echo "$rabbit_mq_text" > docker-compose-dev.yaml

for ((i = 1; i <= $publishers; i++)); do
  client_text="
  publisher$i:
    build:
      context: ./
      dockerfile: test/publisher/publisher.dockerfile
    restart: on-failure
    environment:
      - PYTHONUNBUFFERED=1
      - PUBLISHER_ID=$i
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq"
  echo "$client_text" >> docker-compose-dev.yaml
done

for ((i = 1; i <= $subscribers; i++)); do
  subscriber_text="
  subscriber$i:
    build:
      context: ./
      dockerfile: test/subscriber/subscriber.dockerfile
    restart: on-failure
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - SUBSCRIBER_ID=$i"
  echo "$subscriber_text" >> docker-compose-dev.yaml
done