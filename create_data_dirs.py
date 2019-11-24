import os;

RUN_CONTROL_DATE='20191123'

def createDirsForIntervals(prefix,replace,subdirs):
    i=0
    while i<24:
        firstFile = prefix.replace(replace,RUN_CONTROL_DATE) + ('%02d0000-%02d3000' % (i,i))
        secondFile = prefix.replace(replace,RUN_CONTROL_DATE)
        if i+1 != 24:
            secondFile = secondFile + ('%02d3000-%02d0000' % (i,i+1))
        else:
            secondFile = secondFile + ('%02d3000-000000' % i)

        if not os.path.exists(firstFile):
            os.mkdir(firstFile)
        if not os.path.exists(secondFile):
            os.mkdir(secondFile)
        for subd in subdirs:
            subd1 = firstFile+'/'+subd
            if not os.path.exists(subd1):
                os.mkdir(subd1)
            subd2 = secondFile+'/'+subd
            if not os.path.exists(subd2):
                os.mkdir(subd2)
        i=i+1
    return


paths = [ '/data', '/data/gdelt', '/data/gdelt/RUN_CONTROL_DATE', '/data/db', '/data/db/RUN_CONTROL_DATE']
for p in paths:
    if not os.path.exists(p):
        os.mkdir(p.replace('RUN_CONTROL_DATE',RUN_CONTROL_DATE))

prefix = '/data/gdelt/'+RUN_CONTROL_DATE+'/'
suffixes = ['api','csv','cameo','reject']

for s in suffixes:
    path = prefix+s
    if not os.path.exists(path):
        os.mkdir(path)

createDirsForIntervals('/data/gdelt/RUN_CONTROL_DATE/api/','RUN_CONTROL_DATE',['images','texts'])


def createDummyFiles():
    def jsonFiles():
        path='/data/gdelt/'+RUN_CONTROL_DATE+'/api/'
        for x in [x[0] for x in os.walk(path)]:
            if x.endswith('00'):
                #print(x+'/article_info.json')
                createFile(x+'/article_info.json')
    def cameo():
        pre = '/data/gdelt/'+RUN_CONTROL_DATE+'/cameo/CAMEO._x_.txt';
        s = ['country','type','knowngroup','ethnic','religion','eventcodes']
        for suf in s:
            newFile = pre.replace('_x_',suf)
            createFile(newFile)
            #print(newFile)
    def csv():
        prefix = '/data/gdelt/'+RUN_CONTROL_DATE+'/csv/'
        i=0
        while i<24:
            arr = ['0000','1500','3000','4500']
            for x in arr:
                n_file = prefix+RUN_CONTROL_DATE+('%02d' %i)+x+'.csv'
                createFile(n_file)
                #print(n_file)
            i=i+1
    def createFile(path):
        f = open(path, 'a+')  # open file in append mode
        f.write('Dummy file')
        f.close()

    
    cameo()
    csv()
    jsonFiles()

createDummyFiles()