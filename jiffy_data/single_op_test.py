import csv
import time
from jiffy import JiffyClient
from multiprocessing import Process



def create_connection(hostname, FileName, client):
    queueclient = []
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        qc = client.open_or_create_queue("/" + name_prefix, '/tmp' + name_prefix)
        queueclient.append(qc)
    print("Connection all established")
    return queueclient

def remove_connection(hostname, FileName, client):
    for filename in FileName:
        name_prefix = filename.split('.')[0]
        client.remove("/" + name_prefix)
    print("Connection all closed")
    return


if __name__ == "__main__":
    FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    #FileName = ["jiffy_plan_1.csv"]
    Directory_Server = "172.31.28.29"
    client = JiffyClient(Directory_Server);
    fqs = create_connection(Directory_Server, FileName, client)
    data = 'a' * 128 * 1024
    iteration = 24000
    start =  time.time()
    for i in range(int(iteration / 4)):
        #fqs[0].pipeline_put([data, data, data, data, data, data, data, data, data, data])
        fqs[0].pipeline_put([data, data, data, data])
        #fqs[0].pipeline_put([data, data, data, data, data, data])
        #fqs[0].pipeline_put([data, data, data])
    end1 = time.time()
    for i in range(iteration):
        fqs[0].get()
    end2 = time.time()
    print(end1 - start)
    print(end2 - end1)
    remove_connection(Directory_Server, FileName, client)


