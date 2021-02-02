#!/bin/bash

now=`date '+%Y%m%d'`
echo "${now}"
size="64"
for i in {1..5}
do
    for x in {A..F}
    do
        echo sourav/${now}/pebble/ycsb/size=${size}/run/ycsb_${x}.log
        mkdir -p sourav/${now}/pebble/ycsb/size=${size}/run/
        ./benchTool bench ycsb /tmp/a -c ${size} -w --values 1024 -c 1024 --workload ${x} > sourav/${now}/pebble/ycsb/size=${size}/run/${i}.ycsb_${x}.log
    done
done
