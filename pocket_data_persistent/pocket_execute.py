import csv
import time
import sys
#from multiprocessing import Process, Value, Lock
from multiprocessing import Proces
import pocket

def create_connection(hostname, FileName, ratio, MaxStorage):
    pocketjobID = []
    pocketjobIDnvme = []
    pocket_namenode = pocket.connect(hostname, 9070)
    MaxStorage = {"1": 2, "2": 1.1, "3": 0.9, "4":0.1}
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        fileID = name_prefix.split('_')[-1]
        jobid = pocket.register_job(name_prefix, capacityGB=MaxStorage[fileID] * ratio) # TODO Change this capacityGB
        nvmejobid = pocket.register_job(name_prefix + "_nvme", capacityGB=MaxStorage[fileID])
        pocketjobID.append(jobid)
        pocketjobIDnvme.append(nvmejobid)
    print("Connection all established")
    return pocketjobID, pocketjobIDnvme, pocket_namenode

def remove_connection(pocketID):
    for ID in pocketID:
        pocket.deregister_job(ID)
    print("Connection all closed")
    return

def run_command(cmds, jobid, nvmejobid, tenant_name, namenode, batch_size, max_val, lock, max_size, nvme_list):
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        if(size > batch_size):
            chunks = int(size / batch_size) + 1
            if size % batch_size == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        dram_flag = False
        if op == "put":
            with lock:
                if max_val.value + size < max_size:
                    dram_flag = True
                    max_val.value = max_val.value + size
        for i in range(chunks):
            key = queryID + "_" + str(i)
            if op == "put":
                datasize = min(size, batch_size)
                data = b'a' * datasize
                size = size - datasize
                if(dram_flag):
                    pocket.put_buffer_bytes(namenode, data, len(data), key, jobid)
                else:
                    nvme_list.append(key)
                    pocket.put_buffer_bytes(namenode, data, len(data), key, nvmejobid)
       #         print("Put " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
            elif op == "remove":
                datasize = min(size, batch_size)
                size = size - datasize
                if key in nvme_list:
                    pocket.delete(namenode, key, nvmejobid)
                    nvme_list.remove(key)
                else:
                    dram_flag = True
                    pocket.delete(namenode, key, jobid)
        if op == "remove" and dram_flag:
            with lock:
                max_val.value = max_val.value - size

       #         print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))

def execute(filename, jobID, nvmejobID, execution_plan, namenode, batch_size, max_size):
        nvme_list = []
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, jobID, nvmejobID, tenant_name, namenode, batch_size, max_size, nvme_list)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time.sleep((int(cur_time) - int(prev_time)))
            run_command(command, jobID, nvmejobID, tenant_name, namenode, batch_size, max_size, nvme_list)
            prev_time = cur_time

if __name__ == "__main__":
    FileName = ["pocket_plan_1.csv", "pocket_plan_2.csv", "pocket_plan_3.csv", "pocket_plan_4.csv"]
    HostName = "10.1.0.10"
    ratio = float(sys.argv[1])
    batch_size = 128 * 1024
    #max_val = Value('i', 0)
    #lock = Lock()
    ratio = 1
    MaxStorage = {"1": 2, "2": 1.1, "3": 0.9, "4":0.1}
    #max_size = 3.4 * 1024 * 1024 * 1024 * ratio #FIXME fix this max_size

    jobIDs, nvmejobIDs, namenode = create_connection(HostName, FileName, ratio, MaxStorage)

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
        p = Process(target=execute, args=(FileName[i], jobIDs[i], nvmejobIDs[i], execution[FileName[i]], namenode, batch_size, max_size))
        Pool.append(p)
    start = time.time()
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()
    end = time.time()
    remove_connection(jobIDs + nvmejobIDs)
    print("Execution time: ")
    print(end - start)


