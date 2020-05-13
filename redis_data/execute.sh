#!/bin/bash


./clear_memory.sh
./measure/measure_memory_1.sh &
./measure/measure_memory_2.sh &
./measure/measure_memory_3.sh &
./measure/measure_memory_4.sh &
python redis_test.py &> result.txt
kill -9 $(jobs -p)
./clear_memory.sh



