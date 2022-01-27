import csv
from multiprocessing import Queue

FileNames = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
for i in range(len(FileNames)):
    total = dict()
    for k in range(3601):
        total[k] = 0
    count = 0
    with open(FileNames[i], "r") as f:
        csv_reader = csv.reader(f, delimiter=' ')
        row_count = 0
        for row in csv_reader:

            for cmd in row[1:]:
                op, ID, size = cmd.split(":")
                if op == "put":
                    count = count + int(size)
                if op == "remove":
                    count = count - int(size)
            total[row_count] = total[row_count] + count
            row_count = row_count + 1

    v = 0
    for key in total:
        if int(total[key]) > v:
            v = total[key]

    print(v)
