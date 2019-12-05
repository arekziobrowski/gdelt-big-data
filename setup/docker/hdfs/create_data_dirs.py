import os;
import subprocess

RUN_CONTROL_DATE='20191123'

def dir_exists_hdfs(path):
    output = subprocess.Popen(
        ['hdfs', 'dfs', '-test', '-d', path], stdout=subprocess.PIPE).communicate()[0]
    if output == 0:
        return True
    return False


def mkdir_hdfs(path):
    subprocess.Popen(['hdfs', 'dfs', '-mkdir', '-p', path],
                     stdout=subprocess.PIPE).communicate()[0]

def touch_hdfs(path):
    output = subprocess.Popen(['hdfs', 'dfs', '-touchz', path],
                     stdout=subprocess.PIPE).communicate()[0]

def createDirsForIntervals(prefix,replace,replaceWith, subdirs):
    print('Intervals\' dirs creation:')
    i=0
    while i<24:
        firstFile = prefix.replace(replace,replaceWith) + ('%02d0000-%02d3000' % (i,i))
        secondFile = prefix.replace(replace,replaceWith)
        print(firstFile+' and '+ secondFile)
        if i+1 != 24:
            secondFile = secondFile + ('%02d3000-%02d0000' % (i,i+1))
        else:
            secondFile = secondFile + ('%02d3000-000000' % i)

        if not dir_exists_hdfs(firstFile):
            mkdir_hdfs(firstFile)
        if not dir_exists_hdfs(secondFile):
            mkdir_hdfs(secondFile)
        for subd in subdirs:
            subd1 = firstFile+'/'+subd
            if dir_exists_hdfs(subd1):
                mkdir_hdfs(subd1)
            subd2 = secondFile+'/'+subd
            if not dir_exists_hdfs(subd2):
                mkdir_hdfs(subd2)
        i=i+1

    print('Done')
    return

def generalDirs(date):
    paths = [ '/data', '/data/gdelt', '/data/gdelt/RUN_CONTROL_DATE', '/data/db', '/data/db/RUN_CONTROL_DATE']
    for p in paths:
        print('Creating if not found: '+p)
        if not dir_exists_hdfs(p):
            mkdir_hdfs(p.replace('RUN_CONTROL_DATE',date))

def createDirsForInputData(date):
    prefix = '/data/gdelt/'+date+'/'
    suffixes = ['api','csv','cameo','reject']

    for s in suffixes:
        path = prefix+s
        print('Creating if not found: '+path)
        if not dir_exists_hdfs(path):
            mkdir_hdfs(path)

generalDirs(RUN_CONTROL_DATE)
createDirsForInputData(RUN_CONTROL_DATE)
createDirsForIntervals('/data/gdelt/RUN_CONTROL_DATE/api/','RUN_CONTROL_DATE',RUN_CONTROL_DATE,['images','texts'])
