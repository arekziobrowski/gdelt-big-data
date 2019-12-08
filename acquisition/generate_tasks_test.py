import hdfs_utils as hdfs
import urllib2
import redis

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'
CHECKPOINT_PATH = '/tech/extraction/{DATE}/checkpoint/CHECKPOINT-{DATE}.checkpoint'
MASTREFILE_URL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
DB_DIR = '/data/db/{DATE}'
DATA_DIR = '/data/gdelt/{DATE}'
DATA_SUBDIRS = ['csv','api','cameo','reject']
QUE_NAME = 'CSV_LIST'
REDIS_URL = 'redis-tasks'

def assertExist(path):
    if not hdfs.exists(path):
        print("File '"+path+"' does not exist.")
    else:
        print("'{}'\t\tOK".format(path))

def getCheckpointCount(path):
    checkpointFileContent = hdfs.readFileAsString(path)
    counter = 0
    for line in checkpointFileContent.split('\n')[1:]:
        if '/data/gdelt' in line:
            counter = counter+1
    return counter

def getMasterFileCount(date):
    EXPORT_FILE_SUFFIX_MASTER_FILE = '.export.CSV.zip'
    masterFileReseponse = urllib2.urlopen(MASTREFILE_URL)
    masterFile = masterFileReseponse.read()
    counter = 0
    for line in masterFile.split('\n'):
        if line.endswith(EXPORT_FILE_SUFFIX_MASTER_FILE) and  date in line.split(' ')[2]:
            counter= counter+1

    return counter


def getTaskCount():
    que = redis.Redis(host=REDIS_URL,port=6379)
    size = que.llen(QUE_NAME)
    que.client_kill_filter(_id=que.client_id())
    return size

def assertCAMEOs(path,placeholder):
    TYPES = ['country','type','knowngroup','ethnic','religion','eventcodes']
    for type in TYPES:
        assertExist(path.replace(placeholder,type))

if not hdfs.exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = hdfs.readFileAsString(RUN_CONTROL_PATH)
if DATE.endswith('\n'):
    DATE = DATE[:-1]

assertExist(DB_DIR.replace('{DATE}',DATE))
DATA_DIR = DATA_DIR.replace('{DATE}',DATE)
assertExist(DATA_DIR)
for subdir in DATA_SUBDIRS:
    assertExist(DATA_DIR+'/'+subdir)
assertCAMEOs('/data/gdelt/'+DATE+'/cameo/CAMEO.{type}.txt','{type}')

masterFileCount = getMasterFileCount(DATE)
print("MasterFile records count:\t"+str(masterFileCount))
checkpointCount = getCheckpointCount(CHECKPOINT_PATH.replace('{DATE}',DATE))
print("Checkpoints count:\t"+str(checkpointCount))
taskCount = getTaskCount()
print("Tasks pushed into queue count:\t"+str(taskCount))
if not (masterFileCount-checkpointCount) == taskCount:
    log = "There is #{} checkpoints. There is #{} tasks, instead of #{}".format(checkpointCount,taskCount,(masterFileCount-checkpointCount))
    print(log)