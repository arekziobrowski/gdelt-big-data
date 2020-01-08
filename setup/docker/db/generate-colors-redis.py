# Generates a csv with colors to import

f = open("redis-import.txt", "a")
id = 1
for i in range(256):
    for j in range(256):
        for k in range(256):
            f.write('SET ' + str(i) + ',' + str(j) + ',' + str(k) + ' ' + str(id) + '\n')
            id = id + 1

f.close()
