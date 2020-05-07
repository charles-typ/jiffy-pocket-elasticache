import redis
import time
import sys

FileNames = ["workload1_diff", "workload2_diff", "workload3_diff", "workload4_diff"]

hostname=""
r = redis.Redis(host=hostname, port=6379, db=0)
fileid = int(sys.argv[1])
filename = FileNames[fileid]
commands = []
with open(filename, "r") as f:
    Lines = f.readlines()
    for line in Lines:
        length = int(float((line.strip().split(" ")[1])))
        commands.append(length)
start_time = time.time()
for i in range(len(commands)):
    command = commands[i]
    if command > 0:
        data = "a" * command
        r.set(str(i), data)
    print(commands[i])
    count = 0
    for i in range(10000):
        count = count + 1
    #print("sleep for: " + str(1.0 - ((time.time() - start_time) % 1.0)))
    time.sleep(1.0 - ((time.time() - start_time) % 1.0))
