import csv
from multiprocessing import Queue


FileNames = ["redis_p_1.csv", "redis_p_2.csv", "redis_p_3.csv", "redis_p_4.csv"]
for i in range(len(FileNames)):
    q = Queue()
    with open(FileNames[i], "r") as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            print("hey")
            for cmd in row[1:]:
                op, ID, size = cmd.split(":")
                if op == "put":
                    q.put(int(size))
                elif op == "remove":
                    ret = q.get()
                    assert ret == int(size)
