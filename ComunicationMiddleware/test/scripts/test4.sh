#!/bin/bash
./docker-compose-generator.sh --p 1 --s 3
./run_test.sh  &

#Espero a que levante al container
while ! [[ $(docker ps --filter "name=comunicationmiddleware-subscriber1-1" -q) ]]; do
    sleep 1
done

#Espero a que termine el container 1
while [[ $(docker ps --filter "name=comunicationmiddleware-subscriber1-1" -q) ]]; do
    sleep 1
done

echo 
echo ---------------test multiple subscriber single publisher with different routing keys ---------------
echo

docker cp comunicationmiddleware-subscriber1-1:/out.txt ./test/scripts/out1.txt
if cmp -s "./test/scripts/out1.txt" "./test/scripts/expected_out_1_publisher.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subscriber1\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subscriber1\e[0m"
    diff "./test/scripts/out1.txt" "./test/scripts/expected_out_1_publisher.txt"
fi

docker cp comunicationmiddleware-subscriber2-1:/out.txt ./test/scripts/out2.txt
if cmp -s "./test/scripts/out2.txt" "./test/scripts/expected_out_1_publisher.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subscriber2\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subscriber2\e[0m"
    diff "./test/scripts/out2.txt" "./test/scripts/expected_out_1_publisher.txt"
fi

docker cp comunicationmiddleware-subscriber3-1:/out.txt ./test/scripts/out3.txt
if cmp -s "./test/scripts/out3.txt" "./test/scripts/expected_out_separate_routing_key.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subscriber3\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subscriber3\e[0m"
    diff "./test/scripts/out3.txt" "./test/scripts/expected_out_separate_routing_key.txt"
fi

echo
echo
echo "Para finalizar el test presione enter"
read

rm ./test/scripts/out1.txt
rm ./test/scripts/out2.txt
rm ./test/scripts/out3.txt

./stop_test.sh