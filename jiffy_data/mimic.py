import csv
import queue


FileNames = ["plan_1", "plan_2", "plan_3", "plan_4"]
for i in range(len(FileNames)):
    q = queue.Queue()
    with open(FileNames[i], "r") as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            if row[0] == "put":
                for k in range(2, len(row)):
                    q.put(row[k])
            if row[0] == "remove":
                for k in range(2, len(row), 2):
                    ret = q.get()
                    assert ret == row[k + 1]
