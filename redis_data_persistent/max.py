import csv
from multiprocessing import Queue

FileNames = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
count = 0
for i in range(len(FileNames)):
    with open(FileNames[i], "r") as f:
        csv_reader = csv.reader(f, delimiter=' ')
        row_count = 0
        for row in csv_reader:

            for cmd in row[1:]:
                op, ID, size = cmd.split(":")
                if op == "put":
                    count = count + int(size)
print(count)
