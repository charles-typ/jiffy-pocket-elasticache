import csv
import time
#from jiffy import JiffyClient
from multiprocessing import Process

total = 0

def execute(filename, execution_plan):
    global total
    prev_time = execution_plan[0][0]
    prev_command = execution_plan[0][1:]
    tenant_name = filename.split('.')[0]
    for i in range(1, len(execution_plan)):
        cur_time = execution_plan[i][0]
        command = execution_plan[i][1:]
        #total =  total + (int(cur_time) - int(prev_time))
        for cmd in command:
            op, ID, size = cmd.split(":")
            if(op == "put"):
                total = total + int(size)

if __name__ == "__main__":
    global total
    FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
#    FileName = ["jiffy_plan_1.csv"]
    execution = {}
    for filename in FileName:
        with open(filename) as f:
            execution_plan = []
            csv_reader = csv.reader(f, delimiter=' ')
            prev_command = 0
            for row in csv_reader:
                if(len(row) == 1):
                    continue
                execution_plan.append(row)
        execution[filename] = execution_plan

    for i in range(len(FileName)):
        execute(FileName[i], execution[FileName[i]])

    print(total)

