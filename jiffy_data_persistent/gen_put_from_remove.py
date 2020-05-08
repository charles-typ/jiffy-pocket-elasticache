import csv
import time

RemoveFileNames = ["remove_1", "remove_2", "remove_3", "remove_4"]
CmdFileNames = ["result_1", "result_2", "result_3", "result_4"]
nfiles = 4
for i in range(nfiles):
    put_map = dict()
    with open(RemoveFileNames[i], "r") as f1:
        Lines = f1.readlines()
        for line in Lines:
            tokens = line.strip().split(" ")
            for k in range(2, len(tokens), 2):
                if(tokens[k] not in put_map):
                    put_map[tokens[k]] = [int(tokens[k + 1])]
                else:
                    put_map[tokens[k]].append(int(tokens[k + 1]))
    with open(CmdFileNames[i], "r") as f2:
        out = open("plan_" + str(i + 1), "w")
        csv_reader = csv.reader(f2, delimiter=' ')
        csv_writer = csv.writer(out, delimiter=',')
        for tokens in csv_reader:
            if tokens[0] == "put":
                output_list = []
                output_list.append("put")
                output_list.append(tokens[1])
                if tokens[1] in put_map:
                    output_list = output_list + put_map[tokens[1]]
                    csv_writer.writerow(output_list)
                else:
                    csv_writer.writerow(tokens)
            else:
                if unicode(tokens[-1]).isnumeric():
                    csv_writer.writerow(tokens)
                else:
                    csv_writer.writerow(tokens[0:-1])

