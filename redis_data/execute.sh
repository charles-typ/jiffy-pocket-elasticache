#!/bin/bash


cd measure
./clear_memory.sh
./measure_memory_1.sh 1 &
./measure_memory_2.sh 2 &
./measure_memory_3.sh 3 &
./measure_memory_4.sh 4 & 
cd ..
python redis_test.py &> result.txt
kill -9 $(jobs -p)
cd measure
./clear_memory.sh



