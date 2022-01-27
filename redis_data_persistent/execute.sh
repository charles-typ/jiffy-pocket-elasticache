#!/bin/bash

<<<<<<< HEAD
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 1.0 &> 1_0
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.8 &> 0_8
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.6 &> 0_6
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.4 &> 0_4
./clear_s3.sh
./clear_memory.sh
python redis_execute_persistent.py 0.2 &> 0_2
=======
./clear_s3.sh
./clear_memory.sh
mkdir per_job/1.0
python -u redis_execute_persistent.py 1.0 &> per_job/1.0/1_0
cp *.csv_log per_job/1.0/
./clear_s3.sh
./clear_memory.sh
mkdir per_job/0.8
python -u redis_execute_persistent.py 0.8 &> per_job/0.8/0_8
cp *.csv_log per_job/0.8/
./clear_s3.sh
./clear_memory.sh
mkdir per_job/0.6
python -u redis_execute_persistent.py 0.6 &> per_job/0.6/0_6
cp *.csv_log per_job/0.6/
./clear_s3.sh
./clear_memory.sh
mkdir per_job/0.4
python -u redis_execute_persistent.py 0.4 &> per_job/0.4/0_4
cp *.csv_log per_job/0.4/
./clear_s3.sh
./clear_memory.sh
mkdir per_job/0.2/
python -u redis_execute_persistent.py 0.2 &> per_job/0.2/0_2
cp *.csv_log per_job0.2/
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
./clear_s3.sh
./clear_memory.sh
