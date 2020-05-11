import csv
import time
from redis import StrictRedis
from multiprocessing import Process


def create_connection(HostName):
    rs = []
    for hostname in HostName:
        r1 = StrictRedis(host=hostname, port=6379, db=0)
        #r1 = StrictRedis(host=hostname, port=6379, db=0).pipeline()
        rs.append(r1)
    print("Connection all established")
    return rs





if __name__ == "__main__":

    #FileName = ["new_32571881_plan.csv_norm", "new_32571893_plan.csv_norm", "new_32572121_plan.csv_norm", "new_492868_plan.csv_norm"]
    #FileName = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
    FileName = ["jiffy_plan_1.csv"]
    #FileName = ["32571881_plan.csv_norm", "32571893_plan.csv_norm", "32572121_plan.csv_norm", "492868_plan.csv_norm"]
    HostName = ["ec2-54-69-30-100.us-west-2.compute.amazonaws.com"]

    rs = create_connection(HostName)
    data = "a" * 128 * 1024
    iteration = 24000

    start = time.time()
    for i in range(iteration):
        rs[0].set(str(i), data)
    #for r_s in rs:
    #    r_s.execute()
    end1 = time.time()
    for i in range(iteration):
        rs[0].delete(str(i))
    #for r_s in rs:
    #    r_s.execute()
    end2 = time.time()
    print(end1 - start)
    print(end2 - end1)
