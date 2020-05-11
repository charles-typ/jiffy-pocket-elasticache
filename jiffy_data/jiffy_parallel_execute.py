import csv
import time
from jiffy import JiffyClient
from multiprocessing import Process



def create_connection(hostname, FileName, client, Para):
    queueclient = []
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        paraqueue = []
        for i in range(Para):
            qc = client.open_or_create_queue("/" + name_prefix, '/tmp' + name_prefix)
            paraqueue.append(qc)
        queueclient.append(paraqueue)
    print("Connection all established")
    return queueclient

def remove_connection(hostname, FileName, client):
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        client.remove("/" + name_prefix)
    print("Connection all closed")
    return

def run_command(cmds, fq, tenant_name):
    def write_func(fqclient, data, tail, iteration):
        for i in range(iteration):
            fqclient.put(data)
        if tail > 0:
            data = "a" * tail
            fqclient.put(data)
    def delete_func(fqclient, iteration):
        for i in range(iteration):
            fqclient.get()
    batch_size = 100 * 1024
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        if(size > batch_size):
            chunks = int(size / batch_size) + 1
            if size % (batch_size) == 0:
                chunks = chunks - 1
        else:
            chunks = 1

        data = 'a' * batch_size
        if(op == "put"):
            pool = []
            for i in range(3):
                p = Process(target=write_func, args=(fq[i], data, 0, int(chunks / 4)))
                pool.append(p)
            if(size % batch_size == 0):
                pool.append(Process(target=write_func, args=(fq[3], data, size % batch_size, int(chunks / 4 + chunks % 4))))
            else:
                pool.append(Process(target=write_func, args=(fq[3], data, size % batch_size, int(chunks / 4 + chunks % 4 - 1))))
            for proc in pool:
                proc.start()
            for proc in pool:
                proc.join()
        elif op == "remove":
            for i in range(chunks):
       #         print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(size))
                fq[0].get()




def execute(filename, fq, execution_plan):
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, fq, tenant_name)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            #time.sleep((int(cur_time) - int(prev_time)))
            #time.sleep(1)
            run_command(command, fq, tenant_name)
            prev_time = cur_time


if __name__ == "__main__":

    #FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    Para = 4
    FileName = ["jiffy_plan_1.csv"]
    Directory_Server = "172.31.28.29"
    client = JiffyClient(Directory_Server);
    fqs = create_connection(Directory_Server, FileName, client, Para)
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
    start = time.time()
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()
    end = time.time()
    print("Execution takes time")
    print(end -  start)

    remove_connection(Directory_Server, FileName, client)


