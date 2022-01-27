import csv
import time
from redis import StrictRedis
from multiprocessing import Process, Value, Lock, Queue
import boto3
import sys

#max_val = Value('i', 0)

def create_connection(HostName, S3BucketName):
    rs = []
    s3client = []
    for hostname in HostName:
        r1 = StrictRedis(host=hostname, port=6379, db=0)
        rs.append(r1)
    for i in S3BucketName:
        s3 = boto3.client('s3', 'us-east-2')
        s3client.append(s3)
    print("Connection all established")
    return rs, s3client

<<<<<<< HEAD
def run_command(cmds, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, bucketName, log_file):
=======
def run_command(cmds, rs, tenant_name, max_size, batch_size, s3_list, s3_client, bucketName, log_file, q):
#    global max_val
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
    nrs = len(rs)
    #print(nrs)
    redis_flag = False
    for cmd_id in range(len(cmds)):
        cmd = cmds[cmd_id]
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        original_size = size
        #if(size > max_val):
        #    max_val = size
        if(size > batch_size):
            chunks = int(size / batch_size) + 1
            if size % (batch_size) == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        redis_flag_cur = False
        if op == "put":
<<<<<<< HEAD
            with lock:
                log_file.write(tenant_name + " " + str(max_val.value) + " MAX\n")
                if max_val.value + original_size < max_size:
                    #print("Max value size " + str(max_val.value) )
                    #print("Max size to reach " + str(max_size))
                    #print("increasing " + str(original_size))
                    redis_flag_cur = True
                    max_val.value = max_val.value + original_size
                    log_file.write(tenant_name + " " + str(original_size) + " " + queryID + " WRITE\n")
=======
            #with max_val.get_lock():
            max_val = q.get()
            log_file.write(tenant_name + " " + str(max_val) + " MAX\n")
            if max_val + original_size < max_size:
                #print("Max value size " + str(max_val.value) )
                #print("Max size to reach " + str(max_size))
                #print("increasing " + str(original_size))
                redis_flag_cur = True
                max_val = max_val + original_size
                log_file.write(tenant_name + " " + str(original_size) + " " + queryID + " MEMORY\n")
            q.put(max_val)
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
            if(redis_flag_cur == False):
               # s3_list.append(tenant_name + "_" + queryID)
                log_file.write(tenant_name + " " + str(original_size) + " " + queryID + " PERSISTENT\n")
                s3_list.append(tenant_name + "_" + queryID + "_" + str(original_size))
        remove_from_s3 = False
        if op == "remove" and tenant_name + "_" + queryID + "_" + str(original_size)  in s3_list:
            remove_from_s3 = True
            #s3_list.remove(tenant_name + "_" + queryID)
            s3_list.remove(tenant_name + "_" + queryID + "_" + str(original_size))

        for i in range(chunks):
            #key_name = tenant_name + "_" + queryID + "_" + str(i)
            key_name = tenant_name + "_" + queryID + "_" + str(original_size) + "_" + str(i)
            if op == "put":
                datasize = min(size, batch_size)
                data = 'a' * datasize
        #        print("Put " + key_name + " " + str(datasize))
                size = size - datasize
                idx = int(queryID) % nrs
                if redis_flag_cur:
                    rs[idx].set(key_name, data)
                    #print("Write to redis " + key_name)
                    redis_flag = True
                else:
                    #print("Write to s3 " + key_name)
                    #s3_list.append(key_name)
                    s3_client.put_object(Bucket=bucketName, Key=key_name, Body=data.encode('utf-8'))
            elif op == "remove":
                #datasize = min(size, batch_size)
                #data = 'a' * datasize
                #size = size - datasize
                idx = int(queryID) % nrs
        #        #print("Remove " + key_name + " " + str(size))
                if remove_from_s3:
                    s3_client.delete_object(Bucket=bucketName, Key=key_name)
                    #print("Removing from s3 " + key_name)
                    #s3_list.remove(key_name)
                else:
                    redis_flag = True
                    redis_flag_cur = True
                    rs[idx].delete(key_name)

                    #print("Removing from redis " + key_name)
        if op == "remove" and not remove_from_s3:
<<<<<<< HEAD
            with lock:
                max_val.value = max_val.value - original_size
                log_file.write(tenant_name + " " + str(original_size) + " " + queryID + " DELETE\n")
                #print("Decreasing " + str(original_size))
=======
#            with max_val.get_lock():
            max_val = q.get()
            max_val = max_val - original_size
            #log_file.write(tenant_name + " " + str(original_size) + " " + queryID + " DELETE\n")
            #print("Decreasing " + str(original_size))
            q.put(max_val)
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69





<<<<<<< HEAD
def execute(filename, hostname, rs, execution_plan, max_val, lock, max_size, batch_size, s3_client, s3_bucket):
        log_file = open(filename + "_log", "w")
=======
def execute(filename, hostname, rs, execution_plan, max_size, batch_size, s3_client, s3_bucket, q):
        log_file = open(filename + "_log", "w")
        start_job = time.time()
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
        s3_list = []
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
<<<<<<< HEAD
        run_command(prev_command, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, s3_bucket, log_file)
=======
        run_command(prev_command, rs, tenant_name, max_size, batch_size, s3_list, s3_client, s3_bucket, log_file, q)
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
        for i in range(1, len(execution_plan)):
            print(filename + str(i))
            #print("*******************************************8                          Writing one command " + str(i) + " " + filename)
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            #time_to_sleep = int(cur_time) - int(prev_time)
            #start = time.time()
<<<<<<< HEAD
            run_command(command, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, s3_bucket, log_file)
=======
            run_command(command, rs, tenant_name, max_size, batch_size, s3_list, s3_client, s3_bucket, log_file, q)
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
            #end = time.time()
            #if(end - start < time_to_sleep):
            #    time.sleep(time_to_sleep - end + start)
            prev_time = cur_time
<<<<<<< HEAD
=======
        end_job = time.time()
        print(filename + " " + str(end_job - start_job))
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
        log_file.close()


if __name__ == "__main__":

    #FileName = ["redis_p_1.csv", "redis_p_2.csv", "redis_p_3.csv", "redis_p_4.csv"]
    #FileName = ["redis_p_1.csv"]
    FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
   # FileName = ["jiffy_plan_1.csv"]
    #HostName = ["ec2-18-237-78-196.us-west-2.compute.amazonaws.com", "ec2-34-216-61-41.us-west-2.compute.amazonaws.com"]
<<<<<<< HEAD
    HostName = ["ec2-52-24-220-181.us-west-2.compute.amazonaws.com",
                  "ec2-54-191-224-102.us-west-2.compute.amazonaws.com",
                  "ec2-54-212-229-49.us-west-2.compute.amazonaws.com",
                  "ec2-52-38-9-169.us-west-2.compute.amazonaws.com",
                  "ec2-34-214-106-253.us-west-2.compute.amazonaws.com",
                  "ec2-34-222-251-186.us-west-2.compute.amazonaws.com",
                  "ec2-34-220-6-79.us-west-2.compute.amazonaws.com",
                  "ec2-34-216-49-7.us-west-2.compute.amazonaws.com",
                  "ec2-34-222-90-78.us-west-2.compute.amazonaws.com",
                  "ec2-52-40-138-200.us-west-2.compute.amazonaws.com"]
=======
    HostName = ["ec2-34-217-12-184.us-west-2.compute.amazonaws.com",
    "ec2-52-89-116-219.us-west-2.compute.amazonaws.com",
    "ec2-54-202-68-150.us-west-2.compute.amazonaws.com",
    "ec2-54-200-133-139.us-west-2.compute.amazonaws.com",
    "ec2-34-219-116-63.us-west-2.compute.amazonaws.com",
    "ec2-18-236-153-150.us-west-2.compute.amazonaws.com",
    "ec2-52-12-64-131.us-west-2.compute.amazonaws.com",
    "ec2-52-35-94-85.us-west-2.compute.amazonaws.com",
    "ec2-54-213-240-113.us-west-2.compute.amazonaws.com",
    "ec2-34-212-25-158.us-west-2.compute.amazonaws.com"]
#    "ec2-52-10-250-65.us-west-2.compute.amazonaws.com"]
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69

#    HostName = ["ec2-34-216-61-41.us-west-2.compute.amazonaws.com",
#               "ec2-54-200-251-133.us-west-2.compute.amazonaws.com",
#               "ec2-52-34-187-231.us-west-2.compute.amazonaws.com",
#               "ec2-52-27-238-211.us-west-2.compute.amazonaws.com",
#               "ec2-54-184-125-7.us-west-2.compute.amazonaws.com",
#               "ec2-34-219-16-124.us-west-2.compute.amazonaws.com",
#               "ec2-52-35-251-62.us-west-2.compute.amazonaws.com",
#               "ec2-52-43-242-95.us-west-2.compute.amazonaws.com",
#               "ec2-52-34-194-170.us-west-2.compute.amazonaws.com",
#               "ec2-54-202-113-161.us-west-2.compute.amazonaws.com"]
    S3BucketName = ["redis-p-1", "redis-p-2", "redis-p-3", "redis-p-4"]
    ratio = float(sys.argv[1])
<<<<<<< HEAD
    total_size = 6 * 1024 * 1024 * 1024
    max_val = Value('i', 0)
=======
    total_size = 3.119 * 1024 * 1024 * 1024
>>>>>>> b808cdc363a39996f23633760435b53ecf025d69
    lock = Lock()
    max_size = int(total_size * ratio) # FIXME fix this max_size
    batch_size = 128 * 1024

    rs, s3client = create_connection(HostName, S3BucketName)
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

    q = Queue()
    q.put(0)
    Pool = []
    for i in range(len(FileName)):
        if i == 0:
            servers = rs[0:2]
        elif i == 1:
            servers = rs[2:4]
        elif i == 2:
            servers = rs[4:7]
        elif i == 3:
            servers = rs[7:10]
        p = Process(target=execute, args=(FileName[i], HostName, servers, execution[FileName[i]], max_size, batch_size, s3client[i], S3BucketName[i], q))
        Pool.append(p)
    start = time.time()
    print("Start execution")
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()
    end = time.time()
    print("Exection takes time")
    print(end - start)
