import csv
import time
from multiprocessing import Process
import pocket

def create_connection(hostname, FileName):
    pocketjobID = []
    pocket_namenode = pocket.connect(hostname, 9070)
    MaxStorage = {"1": 1, "2": 1, "3": 0.1, "4":0.01}
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        fileID = name_prefix.split('_')[-1]
        jobid = pocket.register_job(name_prefix, capacityGB=MaxStorage[fileID]) # TODO Change this capacityGB
        pocketjobID.append(jobid)
    print("Connection all established")
    return pocketjobID, pocket_namenode

def remove_connection(pocketID):
    for ID in pocketID:
        pocket.deregister_job(ID)
    print("Connection all closed")
    return

def run_command(cmds, jobid, tenant_name, namenode):
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        if(size > 1024 * 1024):
            chunks = int(size / (1024 * 1024)) + 1
            if size % (1024 * 1024) == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        for i in range(chunks):
            key = queryID + "_" + str(i)
            if op == "put":
                datasize = min(size, 1024 * 1024)
                data = b'a' * datasize
                pocket.put_buffer_bytes(namenode, data, len(data), key, jobid)
       #         print("Put " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
                size = size - datasize
            elif op == "remove":
                datasize = min(size, 1024 * 1024)
                size = size - datasize
       #         print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
                pocket.delete(namenode, key, jobid)
                #r = pocket.get_buffer_bytes(namenode, key, datasize, jobid, DELETE_AFTER_READ=True)

def execute(filename, jobID, execution_plan, namenode):
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, jobID, tenant_name, namenode)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time.sleep((int(cur_time) - int(prev_time)))
            run_command(command, jobID, tenant_name, namenode)
            prev_time = cur_time

if __name__ == "__main__":
    FileName = ["pocket_plan_1.csv", "pocket_plan_2.csv", "pocket_plan_3.csv", "pocket_plan_4.csv"]
    HostName = "10.1.0.10"
    jobIDs, namenode = create_connection(HostName, FileName)
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
        p = Process(target=execute, args=(FileName[i], jobIDs[i], execution[FileName[i]], namenode))
        Pool.append(p)
    start = time.time()
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()
    end = time.time()
    remove_connection(jobIDs)
    print("Execution time: ")
    print(end - start)


