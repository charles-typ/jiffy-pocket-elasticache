import csv


FileNames = ['jiffy_plan_1.csv_log', 'jiffy_plan_2.csv_log', 'jiffy_plan_3.csv_log', 'jiffy_plan_4.csv_log']

for filename in FileNames:
    input_file = open(filename, "r")
    out_dram = open(filename + "_drameval", "w'")
    out_nvme = open(filename + "_nvmeeval", "w'")
    csv_reader = csv.reader(input_file, delimiter = " ")
    csv_writer_dram = csv.writer(out_dram, delimiter = " ")
    csv_writer_nvme = csv.writer(out_nvme, delimiter = " ")
    for row in csv_reader:
        if len(row) == 5 and row[3] == "131072"and row[1] == "put]":
            if float(row[2]) <= 0.002:
                csv_writer_dram.writerow(row)
            else:
                csv_writer_nvme.writerow(row)

