import csv
import sys


Filename = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
MaxSize = [2, 1.1, 0.9, 0.1]
ratio = float(sys.argv[1])


for filename in Filename:
    nvm_list = []
    with open(filename, "r") as f:
        with open(filename + "_pocket", "w") as out:
            csv_reader = csv.reader(f, delimiter = ' ')
            csv_writer = csv.writer(out, delimiter=' ')
            count = 0
            for row in csv_reader:
                for cmd in range(1, len(row)):
                    op, key, size = row[cmd].split(":")
                    if op == "put":
                        if count + int(size) <= MaxSize[int(filename.split(".")[0].split("_")[-1]) - 1] * 1024 * 1024 * 1024 * ratio:
                            count = count + int(size)
                            out.write("Put DRAM " + filename + " " + size + "\n")
                        else:
                            nvm_list.append(key)
                            out.write("Put NVME " + filename + " " + size + "\n")
                    elif op == "remove":
                        if key not in nvm_list:
                            count = count - int(size)
