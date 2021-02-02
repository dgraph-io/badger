#!/bin/bash

mkdir -p data
now=`date '+%Y%m%d'`
echo "${now}"
size="64"
for i in {1..3}
do
    for x in {A..F}
    do
        mkdir -p data/${now}/badger/ycsb/size=${size}/run/
        ./benchTool bench ycsb /tmp/a -c ${size} -w --values ${size} --workload ${x} > data/${now}/badger/ycsb/size=${size}/run/${i}.ycsb_${x}.log
    done
done


echo " ----- Generating Data -----"
./benchTool generateJS data/
