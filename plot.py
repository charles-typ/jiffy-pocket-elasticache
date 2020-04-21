import matplotlib.pyplot as plt


X, Y = [], []
for line in open("workload4_diff", "r"):
    values = [float(s) for s in line.split()]
    X.append(values[0])
    Y.append(values[1])

plt.plot(X, Y)


#X1, Y1 = [], []
#for line in open("workload2_diff", "r"):
#    values = [float(s) for s in line.split()]
#    X1.append(values[0])
#    Y1.append(values[1])
#
#plt.plot(X1, Y1)
#
#X2, Y2 = [], []
#for line in open("workload1_diff", "r"):
#    values = [float(s) for s in line.split()]
#    X2.append(values[0])
#    Y2.append(values[1])
#
#plt.plot(X2, Y2)
#
#X3, Y3 = [], []
#for line in open("workload1_diff", "r"):
#    values = [float(s) for s in line.split()]
#    X3.append(values[0])
#    Y3.append(values[1])
#
#plt.plot(X3, Y3)
plt.show()
