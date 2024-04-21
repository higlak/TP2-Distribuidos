#!/bin/bash
./docker-compose-generator.sh --p 1 --s 2
./run_test.sh  &
sleep 30

echo 
echo ---------------test multiple subscribers single publisher---------------
echo

docker cp comunicationmiddleware-subscriber1-1:/out.txt ./test/scripts/out1.txt
if cmp -s "./test/scripts/out1.txt" "./test/scripts/expected_out_test1.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subsccriber1\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subsccriber1\e[0m"
    diff "./test/scripts/out1.txt" "./test/scripts/expected_out_test1.txt"
fi

docker cp comunicationmiddleware-subscriber2-1:/out.txt ./test/scripts/out2.txt
if cmp -s "./test/scripts/out2.txt" "./test/scripts/expected_out_test1.txt"; then
    echo -e "\e[32mSe recibio correctamente en el subsccriber2\e[0m"
else
    echo -e "\e[31mFallo la recepcion en el subsccriber2\e[0m"
    diff "./test/scripts/out2.txt" "./test/scripts/expected_out_test1.txt"
fi

sleep 10
rm ./test/scripts/out1.txt
rm ./test/scripts/out2.txt

./stop_test.sh