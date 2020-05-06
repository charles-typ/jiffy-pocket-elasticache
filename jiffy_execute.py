import csv
import time
from jiffy import JiffyClient
from multiprocessing import Process



def create_connection(hostname, FileName):
    queueclient = []
    client = JiffyClient(hostname);
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        qc = client.open_or_create_queue(name_prefix, '/tmp' + name_prefix)
        queueclient.append(qc)
    print("Connection all established")
    return queueclient

def run_command(cmds, fq, tenant_name):
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
            if op == "put":
                datasize = min(size, 1024 * 1024)
                data = 'a' * datasize
        #        print("Put " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
                size = size - datasize
                fq.put(data)
            elif op == "remove":
        #        print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(size))
                fq.get()




def execute(filename, fq, execution_plan):
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, fq, tenant_name)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time.sleep((int(cur_time) - int(prev_time)))
            run_command(command, fq, tenant_name)
            prev_time = cur_time


if __name__ == "__main__":

    FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    Directory_Server = ""
    fqs= create_connection(Directory_Server, FileName)
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
        p = Process(target=execute, args=(FileName[i], fqs[i], execution[FileName[i]]))
        Pool.append(p)
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()


