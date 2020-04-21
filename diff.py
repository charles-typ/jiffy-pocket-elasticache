
FileNames = ["workload1", "workload2", "workload3", "workload4"]

time_base = 0
result = dict()
for i in range(20650):
    result[i] = 0.0

for filename in FileNames:
    with open(filename, "r") as f:
        prev = 0.0
        with open(filename+"_diff", "w") as out:
            Lines = f.readlines()
            for line in Lines:
                time = int(line.strip().split(" ")[0])
                if time_base == 0:
                    time_base = int(time)
                data = float(line.strip().split(" ")[1])
                out.write(str(time - time_base) + " " + str(data - prev) + "\n")
                result[time - time_base] = result[time - time_base] + data - prev
                prev = data

with open("summary.txt", "w") as sumout:
    for i in range(20650):
        sumout.write(str(i) + " " + str(result[i]) + "\n")

#fds=[]
#for filename in FileNames:
#    fds.append(open(filename, "r"))
#for i in range(29)
#
#for fd in fds:
#    fd.close()
