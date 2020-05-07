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
    for cmd in cmds:
        [op, queryID, size_str] = cmd.split(":")
        size = int(size_str)
        #if(size > max_val):
        #    max_val = size
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
                idx = int(queryID) % nrs
                rs[idx].set(tenant_name + "_" + queryID + "_" + str(i), data)
            elif op == "remove":
                idx = int(queryID) % nrs
        #        print("Remove " + tenant_name + "_" + queryID + "_" + str(i) + " " + str(size))
                rs[idx].delete(tenant_name + "_" + queryID + "_" + str(i))
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

    FileName = ["new_32571881_plan.csv_norm", "new_32571893_plan.csv_norm", "new_32572121_plan.csv_norm", "new_492868_plan.csv_norm"]
    #FileName = ["32571881_plan.csv_norm", "32571893_plan.csv_norm", "32572121_plan.csv_norm", "492868_plan.csv_norm"]
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
    for proc in Pool:
        proc.start()
    for proc in Pool:
        proc.join()


        #print(max_val)




#time_start = 1520346435
#time_end = 1520367085
#file_name = sys.argv[1]
#tenant_id = file_name.split('.')[0]
#init_query_id = 0
#result = {time_cur: [] for time_cur in range(time_start, time_end)}
#print(file_name)
#with open(file_name) as f:
#    csv_reader = csv.reader(f, delimiter=' ')
#    my_reader = list(csv_reader)
#    for row in my_reader:
#        if(int(row[4]) == 0):
#            continue
#        start_time = max(int(row[2]), time_start)
#        end_time = min(int(row[3]), time_end)
#        for i in range(start_time, end_time + 1):
#            if(i != start_time):
#                result[i].append("remove:" + str(init_query_id) + ":" + str(int(row[4]) / (int(row[3]) - int(row[2]) + 1)))
#            if(i != end_time):
#                result[i].append("put:" + str(init_query_id) + ":" + str(int(row[4]) / (int(row[3]) - int(row[2]) + 1)))
#        init_query_id = init_query_id + 1
#
#out = open(tenant_id + "_plan.csv", "w")
#csv_writer = csv.writer(out, delimiter=' ')
#for k in result:
#    result_list = result[k]
#    job_list = dict()
#    for i in result_list:
#        [op, query_id, size] = i.split(':')
#        if int(query_id) not in job_list:
#            job_list[int(query_id)] = 0
#        job_list[int(query_id)] = job_list[(int(query_id))] + (-1 if op == "remove" else 1) * int(size)
#    output_list = []
#    for i in job_list:
#        if job_list[i] > 0:
#            output_list.append("put:" + str(i) + ":" + str(abs(job_list[i])))
#        if job_list[i] < 0:
#            output_list.append("remove:" + str(i) + ":" + str(abs(job_list[i])))
#    csv_writer.writerow([k] + output_list)
