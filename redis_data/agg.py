import csv


#Filename = ["jiffy_plan_1.csv", "jiffy_plan_2.csv", "jiffy_plan_3.csv", "jiffy_plan_4.csv"]
Filename = ["new_32571881_plan.csv_norm", "new_32571893_plan.csv_norm", "new_32572121_plan.csv_norm", "new_492868_plan.csv_norm"]


for filename in Filename:
    with open(filename, "r") as f:
        with open(filename + "_agg", "w") as out:
            csv_reader = csv.reader(f, delimiter = ' ')
            csv_writer = csv.writer(out, delimiter=' ')
            count = 0
            for row in csv_reader:
                for cmd in range(1, len(row)):
                    op, key, size = row[cmd].split(":")
                    if op == "put":
                        count = count + int(size)
                    elif op == "remove":
                        count = count - int(size)
                csv_writer.writerow([row[0], count])
