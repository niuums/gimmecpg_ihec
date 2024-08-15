#!/bin/bash

files=/home/nchai/ihec/original
out=/home/nchai/NiuzhengChai/gimmecpg/outputs/defaultMode_test/ihec

for file in ${files}/*meth10*
do  
    echo "Imputing $(basename "${file%.*}")"
    python main.py -i ${files} -p $(basename "${file%.*}") -o ${out}/gimmecpg_$(basename "${file%.*}").bed
done
