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

def run_command(cmds, fq, tenant_name, log_file):
    batch_size = 128 * 1024
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        if(size > batch_size):
            chunks = int(size / batch_size) + 1
            if size % (batch_size) == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        #if op == "put":
        #    log_file.write("Put " + tenant_name + "_" + queryID + " " + str(size) + " " + str(chunks) + "\n")
        for i in range(chunks):
            if op == "put":
                datasize = min(size, batch_size)
                data = 'a' * datasize
        #        print("Put " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(datasize))
                size = size - datasize
                start = time.time()
                fq[0].put(data)
                end = time.time()
                log_file.write("[OP put] " + str(end - start) + " " + str(datasize) + " \n")
            elif op == "remove":
                datasize = min(size, batch_size)
                size = size - datasize
        #        print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(size))
                start = time.time()
                fq[0].get()
                end = time.time()
                log_file.write("[OP get] " + str(end - start) + " " + str(datasize) + " \n")





def execute(filename, fq, execution_plan):
        log_file = open(filename + "_log", "w")
        start_job = time.time()
        prev_time = execution_plan[0][0]
        prev_command = execution_plan[0][1:]
        tenant_name = filename.split('.')[0]
        run_command(prev_command, fq, tenant_name, log_file)
        for i in range(1, len(execution_plan)):
            cur_time = execution_plan[i][0]
            command = execution_plan[i][1:]
            time_to_sleep = int(cur_time) - int(prev_time)
            #time.sleep((int(cur_time) - int(prev_time)))
            #time.sleep(1)
            start = time.time()
            run_command(command, fq, tenant_name, log_file)
            end = time.time()
            #if(end - start < time_to_sleep):
            #    time.sleep(time_to_sleep - end + start)
            prev_time = cur_time
        end_job = time.time()
        print(filename + " " + str(end_job - start_job))
        log_file.close()


if __name__ == "__main__":

    FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    Para = 1
    #FileName = ["jiffy_plan_1.csv"]
    Directory_Server = "172.31.21.109"
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


