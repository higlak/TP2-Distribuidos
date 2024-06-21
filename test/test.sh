#!/bin/bash
./run.sh  &

#Espero a que levante al container
while ! [[ $(docker ps --filter "name=tp2-distribuidos-client0-1" -q) ]]; do
    sleep 1
done

#Espero a que termine el container 0
while [[ $(docker ps --filter "name=tp2-distribuidos-client0-1" -q) ]]; do
    sleep 1
done

#Espero a que termine el container 1
while [[ $(docker ps --filter "name=tp2-distribuidos-client1-1" -q) ]]; do
    sleep 1
done

./stop.sh

echo 
echo ---------------test solo query 3 ejemplo chico 2 clientes---------------
echo

resultado=0

if cmp -s "./test/result3.csv" "./data/client0/result3.csv"; then
    echo
    echo -e "\e[32mSe recibio correctamente el primer cliente\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion en el primer cliente\e[0m"
    echo
    resultado=$((resultado + 1))
fi

if cmp -s "./test/result3.csv" "./data/client0/result3.csv"; then
    echo
    echo -e "\e[32mSe recibio correctamente el segundo cliente\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion en el segundo cliente\e[0m"
    echo
    resultado=$((resultado + 2))
fi

directorio="./persistance_files/"
no_borrar1="log_type3.1.0.txt"
no_borrar2="log_type3.0.0.txt"

if [ "$resultado" -eq 0 ]; then
    if [ -d "$directorio" ]; then
        echo "Borrando persistencia "
        find "$directorio" -type f ! -name "$no_borrar1" ! -name "$no_borrar2" -exec rm {} \;
    else
        echo "No encontre persistencia"
    fi
fi

exit $resultado
