#!/bin/bash

#./clear_s3.sh
#./clear_memory.sh
#python redis_execute_persistent.py 1.0 &> 1_0
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.8 &> 0_8
#./clear_s3.sh
#./clear_memory.sh
#python redis_execute_persistent.py 0.6 &> 0_6
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.4 &> 0_4
#./clear_s3.sh
#./clear_memory.sh
#python redis_execute_persistent.py 0.2 &> 0_2
./clear_s3.sh
./clear_memory.sh
