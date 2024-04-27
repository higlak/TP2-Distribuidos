#!/bin/bash
./docker-compose-generator.sh --p 1 --c 2
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

docker cp communicationmiddleware-consumer1-1:/out.txt ./test/scripts/out1.txt
docker cp communicationmiddleware-consumer2-1:/out.txt ./test/scripts/out2.txt

lines_file1=$(wc -l < ./test/scripts/out1.txt)
lines_file2=$(wc -l < ./test/scripts/out2.txt)
total_lines=$((lines_file1 + lines_file2))
if [ "$total_lines" -eq 10 ]; then
    echo
    echo -e "\e[32mSe reciben en total los 10 mensajes entre los dos consumidores1\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion\e[0m"
    echo
fi

echo
echo
echo "Para finalizar el test presione enter"
read

rm ./test/scripts/out1.txt
rm ./test/scripts/out2.txt

./stop_test.sh