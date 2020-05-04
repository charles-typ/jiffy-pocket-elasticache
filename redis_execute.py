import csv
import sys
import time

FileName = ["32571881_plan.csv_norm", "32571893_plan.csv_norm", "32572121_plan.csv_norm", "492868_plan.csv_norm"]
HostName = ["ec2-54-244-209-229.us-west-2.compute.amazonaws.com"]

nrs = len(HostName)
nrs = 10

exec_id = int(sys.argv[1])
delete_map = dict()
rs = 0
#max_val = 0
#max_grp = [0] * 10

def run_command(cmds):
    #global max_val
    global nrs, HostName, rs, max_grp
    for cmd in cmds:
        [op, queryID, size] = cmd.split(":")
        size = int(size)
        #if(size > max_val):
        #    max_val = size
        if(size > 1024 * 1024):
            chunks = size / 1024 / 1024 + 1
            if size % (1024 * 1024) == 0:
                chunks = chunks - 1
        else:
            chunks = 1
        for i in range(chunks):
            if op == "put":
                datasize = min(size, 1024 * 1024)
                data = 'a' * datasize
                print("Put " + queryID + "_" + str(i) + " " + str(datasize))
                size = size - datasize
                idx = int(queryID) % nrs
            #rs.set(queryID, data)
            elif op == "remove":
                idx = int(queryID) % nrs
                print("Remove " + queryID + "_" + str(i) + " " + str(size))
            #rs.delete(queryID)


with open(FileName[exec_id]) as f:
    execution_plan = []
    csv_reader = csv.reader(f, delimiter=' ')
    prev_command = 0
    for row in csv_reader:
        if(len(row) == 1):
            continue
        execution_plan.append(row)


    prev_time = execution_plan[0][0]
    prev_command = execution_plan[0][1:]
    run_command(prev_command)
    for i in range(1, len(execution_plan)):
        cur_time = execution_plan[i][0]
        command = execution_plan[i][1:]
        #time.sleep(int(cur_time) - int(prev_time))
        run_command(command)
        prev_time = cur_time


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
