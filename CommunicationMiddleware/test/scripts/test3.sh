#!/bin/bash
./docker-compose-generator.sh --p 2 --s 2
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
echo ---------------test multiple subscriber miltiple publisher---------------
echo

docker cp comunicationmiddleware-subscriber1-1:/out.txt ./test/scripts/out1.txt
if cmp -s "./test/scripts/out1.txt" "./test/scripts/expected_out_2_publishers.txt"; then
    echo
    echo -e "\e[32mSe recibio correctamente en el subscriber1\e[0m"
else
    echo
    echo -e "\e[31mFallo la recepcion en el subscriber1\e[0m"
    diff "./test/scripts/out1.txt" "./test/scripts/expected_out_2_publishers.txt"
fi

docker cp comunicationmiddleware-subscriber2-1:/out.txt ./test/scripts/out2.txt
if cmp -s "./test/scripts/out2.txt" "./test/scripts/expected_out_2_publishers.txt"; then
    echo
    echo -e "\e[32mSe recibio correctamente en el subscriber2\e[0m"
else
    echo
    echo -e "\e[31mFallo la recepcion en el subsccriber2\e[0m"
    diff "./test/scripts/out2.txt" "./test/scripts/expected_out_2_publishers.txt"
fi

echo
echo
echo "Para finalizar el test presione enter"
read

rm ./test/scripts/out1.txt
rm ./test/scripts/out2.txt

./stop_test.sh