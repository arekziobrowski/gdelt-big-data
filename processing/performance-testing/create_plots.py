import matplotlib.pyplot as plt

FILENAME = "sample_results.txt"

with open(FILENAME, "r") as file:
    lines = file.readlines()

    timestamp = lines[0].strip()
    tests = [x.strip() for x in lines[1].split("\t")[1:]]
    data = [x.strip() for x in lines[2:]]

    results = list()
    hours = list()

    for line in data:
        splitted = line.split("\t")
        hours.append(int(splitted[0]))
        results.append([float(x) for x in splitted[1:]])

    transposed = map(list, zip(*results))

figure = plt.figure()
for result_set in transposed:
    plt.plot(hours, result_set)
plt.title('Results {}'.format(timestamp))
plt.xlabel('Hour period')
plt.ylabel('Seconds')
plt.legend(tests, loc=2)
plt.show()
figure.savefig("results-{}.png".format(timestamp))
