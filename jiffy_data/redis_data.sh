#!/bin/bash


python3 gen_redis.py 0 > result_1
python3 gen_redis.py 1 > result_2
python3 gen_redis.py 2 > result_3
python3 gen_redis.py 3 > result_4
for i in {1..4} 
do
    cat result_${i} | grep "remove" > remove_${i}
done
