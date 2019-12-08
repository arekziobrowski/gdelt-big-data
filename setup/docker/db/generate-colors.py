# Generates a csv with colors to import

f = open("color_metadata.csv", "a")
id = 1
for i in range(10):
    for j in range(10):
        for k in range(10):
            f.write('"' + str(id) + '","' + str(i) + '","' + str(j) + '","' + str(k) + '"' + '\n')
            id = id + 1

f.close()
