#!/bin/bash

times_run=0

while true; do
    echo hola
    ./test/test.sh
    # Verificar el código de salida de test.sh
    if [ $? -eq 0 ]; then
        echo "SAlio bien, corrida nro:$times_run"
    else
        echo "Salio mal cortando loop"
        break
    fi

    times_run=$((times_run + 1))
    sleep 2
    
done