import csv
import time
from redis import StrictRedis
from multiprocessing import Process


def create_connection(HostName):
    rs = []
    for hostname in HostName:
        r1 = StrictRedis(host=hostname, port=6379, db=0).pipeline()
        rs.append(r1)
    print("Connection all established")
    return rs

def run_command(cmds, rs, tenant_name):
    nrs = len(rs)
    batch_size = 1024 * 1024
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        original_size = size
        #if(size > max_val):
        #    max_val = size
        if(size > batch_size):
            chunks = int(size / (batch_size)) + 1
            if size % (batch_size) == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        for i in range(chunks):
            if op == "put":
                datasize = min(size, batch_size)
                data = 'a' * datasize
        #        print("Put " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
                size = size - datasize
                idx = int(queryID) % nrs
                #rs[idx].set(tenant_name + "_" + queryID + "_" + str(i), data)
                rs[idx].set(tenant_name + "_" + queryID + "_" + str(original_size) + "_" + str(i), data)
            elif op == "remove":
                idx = int(queryID) % nrs
        #        print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(size))
                #rs[idx].delete(tenant_name + "_" + queryID + "_" + str(i))
                rs[idx].delete(tenant_name + "_" + queryID + "_" + str(original_size) + "_" + str(i))
    for r in rs:
        r.execute()




def execute(filename, hostname, rs, execution_plan):
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('_')[0]
        run_command(prev_command, rs, tenant_name)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time.sleep((int(cur_time) - int(prev_time)))
            run_command(command, rs, tenant_name)
            prev_time = cur_time


if __name__ == "__main__":

    #FileName = ["new_32571881_plan.csv_norm", "new_32571893_plan.csv_norm", "new_32572121_plan.csv_norm", "new_492868_plan.csv_norm"]
    #FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    FileName = ["redis_full_plan_1.csv", "redis_full_plan_2.csv", "redis_full_plan_3.csv", "redis_full_plan_4.csv"]
    #FileName = ["32571881_plan.csv_norm", "32571893_plan.csv_norm", "32572121_plan.csv_norm", "492868_plan.csv_norm"]
	HostName = ["ec2-34-209-37-155.us-west-2.compute.amazonaws.com",
	"ec2-34-212-249-209.us-west-2.compute.amazonaws.com",
	"ec2-18-237-117-242.us-west-2.compute.amazonaws.com",
	"ec2-35-165-117-136.us-west-2.compute.amazonaws.com",
	"ec2-35-167-253-133.us-west-2.compute.amazonaws.com",
	"ec2-34-219-58-19.us-west-2.compute.amazonaws.com",
	"ec2-52-40-54-93.us-west-2.compute.amazonaws.com",
	"ec2-54-187-181-210.us-west-2.compute.amazonaws.com",
	"ec2-34-220-211-19.us-west-2.compute.amazonaws.com",
	"ec2-54-203-91-79.us-west-2.compute.amazonaws.com"]

    rs = create_connection(HostName)
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
        p = Process(target=execute, args=(FileName[i], HostName, servers, execution[FileName[i]]))
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
