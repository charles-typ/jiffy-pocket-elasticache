import csv
import time
from redis import StrictRedis
from multiprocessing import Process, Value, Lock
import boto3


def create_connection(HostName, S3BucketName):
    rs = []
    s3client = []
    for hostname in HostName:
        r1 = StrictRedis(host=hostname, port=6379, db=0).pipeline()
        rs.append(r1)
    for i in S3BucketName:
        s3 = boto3.client('s3', 'us-east-2')
        s3client.append(s3)
    print("Connection all established")
    return rs, s3client

def run_command(cmds, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, bucketName):
    nrs = len(rs)
    redis_flag = False
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
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
            with lock:
                if max_val.value + size < max_size:
                    redis_flag_cur = True
                    max_val.value = max_val.value + size
        for i in range(chunks):
            key_name = tenant_name + "_" + queryID + "_" + str(i)
            if op == "put":
                datasize = min(size, batch_size)
                data = 'a' * datasize
        #        print("Put " + key_name + " " + str(datasize))
                size = size - datasize
                idx = int(queryID) % nrs
                if redis_flag_cur:
                    rs[idx].set(key_name, data)
                    redis_flag = True
                else:
                    s3_list.append(key_name)
                    s3_client.put_object(Bucket=bucketName, Key=key_name, Body=data.encode('utf-8'))
            elif op == "remove":
                datasize = min(size, batch_size)
                data = 'a' * datasize
                size = size - datasize
                idx = int(queryID) % nrs
        #        print("Remove " + key_name + " " + str(size))
                if key_name in s3_list:
                    s3_client.delete_object(Bucket=bucketName, Key=key_name)
                    s3_list.remove(key_name)
                else:
                    redis_flag = True
                    redis_flag_cur = True
                    rs[idx].delete(tenant_name + "_" + queryID + "_" + str(i))
        if op == "remove" and redis_flag_cur:
            with lock:
                max_val.value = max_val.value - size

    if redis_flag is True:
        for r in rs:
            r.execute()




def execute(filename, hostname, rs, execution_plan, max_val, lock, max_size, batch_size, s3_client, s3_bucket):
        s3_list = []
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, s3_bucket)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time.sleep((int(cur_time) - int(prev_time)))
            run_command(command, rs, tenant_name, max_val, lock, max_size, batch_size, s3_list, s3_client, s3_bucket)
            prev_time = cur_time


if __name__ == "__main__":

    FileName = ["redis_p_1.csv", "redis_p_2.csv", "redis_p_3.csv", "redis_p_4.csv"]

    HostName = ["ec2-54-149-159-4.us-west-2.compute.amazonaws.com",
    "ec2-54-245-193-27.us-west-2.compute.amazonaws.com",
    "ec2-34-221-168-60.us-west-2.compute.amazonaws.com",
    "ec2-54-201-173-208.us-west-2.compute.amazonaws.com",
    "ec2-34-221-179-53.us-west-2.compute.amazonaws.com",
    "ec2-54-190-195-23.us-west-2.compute.amazonaws.com",
    "ec2-35-160-108-71.us-west-2.compute.amazonaws.com",
    "ec2-34-216-21-74.us-west-2.compute.amazonaws.com",
    "ec2-34-222-48-204.us-west-2.compute.amazonaws.com",
    "ec2-54-214-109-51.us-west-2.compute.amazonaws.com"]
    S3BucketName = ["redis_p_1", "redis_p_2", "redis_p_3", "redis_p_4"]

    max_val = Value('i', 0)
    lock = Lock()
    ratio = 1
    max_size = 3.4 * 1024 * 1024 * ratio # FIXME fix this max_size
    batch_size = 1024 * 1024

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
        p = Process(target=execute, args=(FileName[i], HostName, servers, execution[FileName[i]], max_val, lock, max_size, batch_size, s3client[i], S3BucketName[i]))
        Pool.append(p)
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()

