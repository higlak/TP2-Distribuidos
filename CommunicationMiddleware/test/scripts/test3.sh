#!/bin/bash
./docker-compose-generator.sh --p 2 --c 1
./run_test.sh  &

#Espero a que levante al container
while ! [[ $(docker ps --filter "name=communicationmiddleware-producer1-1" -q) ]]; do
    sleep 1
done

#Espero a que termine el container 1
while [[ $(docker ps --filter "name=communicationmiddleware-producer1-1" -q) ]]; do
    sleep 1
done

echo "
Se finalizo de enviar y recibir mensajes
"

echo 
echo ---------------test multiple consumers single producer ---------------
echo

docker cp communicationmiddleware-consumer1-1:/out.txt ./test/scripts/out.txt
if cmp -s "./test/scripts/out.txt" "./test/scripts/expected_out_2_producers.txt"; then
    echo
    echo -e "\e[32mSe recibio correctamente en el consumer1\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion en el consumer1\e[0m"
    echo
    diff "./test/scripts/out.txt" "./test/scripts/expected_out_1_consumer.txt"
fi

echo
echo
echo "Para finalizar el test presione enter"
read

rm ./test/scripts/out.txt

./stop_test.sh