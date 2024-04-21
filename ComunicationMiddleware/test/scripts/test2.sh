#!/bin/bash
./docker-compose-generator.sh --p 2 --s 1
./run_test.sh  &
sleep 30

echo 
echo ---------------test single subscriber miltiple publisher---------------
echo

docker cp comunicationmiddleware-subscriber1-1:/out.txt ./test/scripts/out.txt
if cmp -s "./test/scripts/out.txt" "./test/scripts/expected_out_test2.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subsccriber1\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subsccriber1\e[0m"
    diff "./test/scripts/out.txt" "./test/scripts/expected_out_test2.txt"
fi

sleep 10
rm ./test/scripts/out.txt

./stop_test.sh