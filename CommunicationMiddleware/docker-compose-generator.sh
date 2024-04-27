#!/bin/bash
producers="1"
consumers="2"

echo $0, $1, $2

while [[ $# -gt 0 ]]; do #cantidad de argumentos >= 0
    key="$1"
    case $key in
        --p)
            producers="$2"
            shift 2       #move los argumentos 2 posiciones
            ;;
        --c)
            consumers="$2"
            shift 2
            ;;
        *)
            echo "--p P --c C son los args donde P es # de producers y C # de consumers"
            exit
            ;;
    esac
done

echo "Se levantaran $producers producers y consumer $consumers"

rabbit_mq_text="version: '3'
services:
  rabbitmq:
    build:
      context: ./test/rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672"

echo "$rabbit_mq_text" > docker-compose-dev.yaml

for ((i = 1; i <= $producers; i++)); do
  client_text="
  producer$i:
    build:
      context: ./
      dockerfile: test/producer/producer.dockerfile
    restart: on-failure
    environment:
      - PYTHONUNBUFFERED=1
      - PRODUCER_ID=$i
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq"
  echo "$client_text" >> docker-compose-dev.yaml
done

for ((i = 1; i <= $consumers; i++)); do
  consumer_text="
  consumer$i:
    build:
      context: ./
      dockerfile: test/consumer/consumer.dockerfile
    restart: on-failure
    depends_on:
      - rabbitmq
    links: 
      - rabbitmq
    environment:
      - PYTHONUNBUFFERED=1
      - CONSUMER_ID=$i"
  echo "$consumer_text" >> docker-compose-dev.yaml
done