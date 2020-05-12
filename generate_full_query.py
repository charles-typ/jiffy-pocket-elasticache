import csv
import sys

time_start = 1520347400
time_end = 1520351000
file_name = sys.argv[1]
tenant_id = file_name.split('.')[0]
init_query_id = 0
result = {time_cur: [] for time_cur in range(time_start, time_end + 1)}
print(file_name)
with open(file_name) as f:
    csv_reader = csv.reader(f, delimiter=' ')
    my_reader = list(csv_reader)
    for row in my_reader:
        if(int(row[4]) == 0):
            continue
        start_time = max(int(row[2]), time_start)
        end_time = min(int(row[3]), time_end)
        put_count = 0
        for i in range(start_time, end_time + 1):
            if(i != start_time):
                result[i].append("remove:" + str(init_query_id) +  "_" + str(put_count - 1) + ":" + str(int(int(row[4]) / (int(row[3]) - int(row[2]) + 1))))
            if(i != end_time):
                result[i].append("put:" + str(init_query_id) + "_" + str(put_count) + ":" + str(int(int(row[4]) / (int(row[3]) - int(row[2]) + 1))))
                put_count = put_count + 1
        init_query_id = init_query_id + 1

out = open(tenant_id + "_test.csv", "w")
csv_writer = csv.writer(out, delimiter=' ')
for i in range(time_start, time_end + 1):
    csv_writer.writerow([str(i - time_start)] + result[i])

