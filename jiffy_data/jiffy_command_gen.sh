#!/bin/bash

# Four workload files workload1~4

# Get storage difference between every two rows
python diff.py
# Generate put, remove command based on difference. (Bytes)
python3 gen_redis.py 0 > result_1
python3 gen_redis.py 1 > result_2
python3 gen_redis.py 2 > result_3
python3 gen_redis.py 3 > result_4

# These steps are basically changing the workload to key-value style instead of using bytes directly
# The result will be: put key value, remove key
for i in {1..4} 
do
    cat result_${i} | grep "remove" > remove_${i}
done
python gen_put_from_remove.py
python jiffy-ec-transfer.py

# The final results are stored in jiffy_plan_1~4.csv
