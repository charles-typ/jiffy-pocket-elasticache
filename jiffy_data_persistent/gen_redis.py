#import redis
import time
import sys

FileNames = ["workload1_diff", "workload2_diff", "workload3_diff", "workload4_diff"]

hostname=""
reduce_map = dict()
#put_map = dict()
#r = redis.Redis(host=hostname, port=6379, db=0)
fileid = int(sys.argv[1])
filename = FileNames[fileid]
commands = []
with open(filename, "r") as f:
    Lines = f.readlines()
    for line in Lines:
        length = int(float((line.strip().split(" ")[1])))
        commands.append(length)
start_time = time.time()
reduce_key = []
reduce_id = 0
for i in range(len(commands)):
    command = commands[i]
    if command > 0:
        data = "a" * command
        reduce_map[i] = command
        #put_map[i] = dict()
        reduce_key.append(i)
        print("put " + str(i) + " " + str(command))
    elif command < 0:
        print("remove " + str(i) + " ", end = '')
        flag = False
        while(reduce_map[reduce_key[reduce_id]] <= 0 - command):
            flag = True
            tmp = reduce_map[reduce_key[reduce_id]]
            command += tmp
            reduce_map[reduce_key[reduce_id]] = 0
            print(str(reduce_key[reduce_id]) + " " + str(tmp) + " ", end = '')
            reduce_id = reduce_id + 1
            try:
                test = reduce_key[reduce_id]
            except Exception:
                break
        if(command != 0):
            flag = False
            tmp = reduce_map[reduce_key[reduce_id]]
            reduce_map[reduce_key[reduce_id]] = tmp + command
            print(str(reduce_key[reduce_id]) + " " + str(0 - command))
            command = 0
        if flag is True:
            print("\n", end = '')
        if(command < 0):
           raise Exception("Should not happen")
    else:
        print("NULL " + str(i))

#        r.set(str(i), data)
    #print(commands[i])
    #count = 0
    #for i in range(10000):
    #    count = count + 1
    #print("sleep for: " + str(1.0 - ((time.time() - start_time) % 1.0)))
    #time.sleep(1.0 - ((time.time() - start_time) % 1.0))
