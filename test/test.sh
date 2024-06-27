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
resultado_cliente0="./data/client4/result2.csv"
resultado_cliente1="./data/client5/result2.csv"
sort "$resultado_cliente0" -o "$resultado_cliente0"
sort "$resultado_cliente1" -o "$resultado_cliente1"

if cmp -s "./test/result2.csv" "$resultado_cliente0"; then
    echo
    echo -e "\e[32mSe recibio correctamente el primer cliente\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion en el primer cliente\e[0m"
    echo
    resultado=$((resultado + 1))
fi

if cmp -s "./test/result2.csv" "$resultado_cliente1"; then
    echo
    echo -e "\e[32mSe recibio correctamente el segundo cliente\e[0m"
    echo
else
    echo
    echo -e "\e[31mFallo la recepcion en el segundo cliente\e[0m"
    echo
    resultado=$((resultado + 2))
fi

directorio1="./persistance_files/"
directorio2="./persistanceGateway/"
no_borrar1="log_type3.1.0.txt"
no_borrar2="log_type3.0.0.txt"

if [ "$resultado" -eq 0 ]; then
    if [ -d "$directorio1" ]; then
        echo "Borrando persistencia "
        find "$directorio1" -type f ! -name "$no_borrar1" ! -name "$no_borrar2" -exec rm {} \;
    else
        echo "No encontre persistencia"
    fi
fi

if [ "$resultado" -eq 0 ]; then
    if [ -d "$directorio2" ]; then
        echo "Borrando persistencia "
        find "$directorio2" -type f ! -name "log_type.txt" -exec rm {} \;
    else
        echo "No encontre persistencia"
    fi
fi

exit $resultado
