import csv

Filenames = ["plan_1", "plan_2", "plan_3", "plan_4"]
nf = len(Filenames)

for i in range(nf):
    with open(Filenames[i], "r") as f:
        out = open("jiffy_plan_" + str(i + 1) + ".csv", "w")
        csv_reader = csv.reader(f, delimiter=',')
        csv_writer = csv.writer(out, delimiter=' ')
        for row in csv_reader:
            output_row = []
            output_row.append(row[1])
            if(row[0] == "put"):
                for k in range(2, len(row)):
                    output_row.append("put:" + row[1] + ":" + row[k])
            elif(row[0] == "remove"):
                for k in range(2, len(row), 2):
                    output_row.append("remove:" + row[k] + ":" + row[k+1])
            csv_writer.writerow(output_row)


