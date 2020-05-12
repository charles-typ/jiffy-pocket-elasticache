#!/bin/bash


python generate_full_query.py 32571881.csv_sorted && cp 32571881_test.csv jiffy_full_plan_2 &&  cp 32571881_test.csv pocket_full_plan_2 &&  cp 32571881_test.csv redis_full_plan_2
python generate_full_query.py 32571893.csv_sorted &&  cp 32571893_test.csv jiffy_full_plan_1 && cp 32571893_test.csv pocket_full_plan_1 &&  cp 32571893_test.csv redis_full_plan_1 
python generate_full_query.py 32572121.csv_sorted &&  cp 32572121_test.csv jiffy_full_plan_4  &&  cp 32572121_test.csv pocket_full_plan_4 &&  cp 32572121_test.csv redis_full_plan_4
python generate_full_query.py 492868.csv_sorted &&  cp 492868_test.csv jiffy_full_plan_3 &&  cp 492868_test.csv pocket_full_plan_3 &&  cp 492868_test.csv redis_full_plan_3
