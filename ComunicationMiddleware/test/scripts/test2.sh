#!/bin/bash
./docker-compose-generator.sh --p 2 --s 1
./run_test.sh  &

#Espero a que levante al container
while ! [[ $(docker ps --filter "name=comunicationmiddleware-publisher1-1" -q) ]]; do
    sleep 1
done

#Espero a que termine el container 1
while [[ $(docker ps --filter "name=comunicationmiddleware-publisher1-1" -q) ]]; do
    sleep 1
done

echo 
echo ---------------test single subscriber miltiple publisher---------------
echo

docker cp comunicationmiddleware-subscriber1-1:/out.txt ./test/scripts/out.txt
if cmp -s "./test/scripts/out.txt" "./test/scripts/expected_out_2_publishers.txt"; then
    echo
    echo -e "\e[32mSe recibio correctamente en el subscriber1\e[0m"
else
    echo
    echo -e "\e[31mFallo la recepcion en el subscriber1\e[0m"
    diff "./test/scripts/out.txt" "./expected_out_2_publishers.txt"
fi

echo
echo
echo "Para finalizar el test presione enter"
read
rm ./test/scripts/out.txt

./stop_test.sh