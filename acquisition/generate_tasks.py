import hdfs_utils as hdfs
import urllib2
import datetime
import redis
import sys

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'
REDIS_URL = 'redis-tasks'
CHECKPOINT_PATH = '/tech/extraction/{DATE}/checkpoint/CHECKPOINT-{DATE}.checkpoint'
MASTREFILE_URL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
DATA_DIR = '/data/gdelt/{DATE}'
DB_DIR = '/data/db/{DATE}'
DATA_SUBDIRS = ['csv','api','cameo','reject']
LOG_PATH = '/tech/extraction/{DATE}/log'
QUE_NAME = 'CSV_LIST'

def getCheckpointsList(path,RUN_CONTROL_DATE):
    EXPORT_FILE_SUFFIX = '.export.csv'
    path = path.replace('{DATE}',str(RUN_CONTROL_DATE))
    if not hdfs.exists(path):
        return []

    checkpointFileContent = hdfs.readFileAsString(path)
    checkpointList = []
    for line in checkpointFileContent.split('\n')[1:] :
        if line == '':
            continue

        if not line.endswith(EXPORT_FILE_SUFFIX):
            errorMessage = str.format('"%s" does not end with the suffix ".export.csv"',line)
            print(line)
            hdfs.log(LOG_PATH,errorMessage,True)
        else:
            splitted_line = line.split('/')
            pathDate = splitted_line[3]
            if pathDate == RUN_CONTROL_DATE:
                fileName = splitted_line[5].split('.')[0]
                checkpointList.append(fileName)
    return checkpointList

def generate_daily_time_slots(CONTROL_DATE, end_hour=24):
    base_slots = []
    for hour in [str(h).zfill(2) for h in range(0, int(end_hour))]:
        for minutes in ["00", "15", "30", "45"]:
            time = "{0}{1}{2}".format(hour, minutes, "00")
            base_slots.append(CONTROL_DATE + time)

    return base_slots

def getNewTasksList(CONTROL_DATE, CHECKPOINT_PATH, MASTREFILE_URL, end_hour=24):
    time_stamps_to_download = generate_daily_time_slots(CONTROL_DATE, end_hour)

    EXPORT_FILE_SUFFIX_MASTER_FILE = '.export.CSV.zip'

    checkPointList = getCheckpointsList(CHECKPOINT_PATH,CONTROL_DATE)
    masterFileReseponse = urllib2.urlopen(MASTREFILE_URL)
    masterFile = masterFileReseponse.read()
    counter = 0
    taskList = []
    for line in masterFile.split('\n'):
        gdeltRecord = line.split(' ')
        if len(gdeltRecord) != 3:
            hdfs.log(LOG_PATH,'Invalid record in GDELT MASTER FILE',False)
            continue
        if CONTROL_DATE in gdeltRecord[2] and gdeltRecord[2].endswith(EXPORT_FILE_SUFFIX_MASTER_FILE):
            for slot in time_stamps_to_download:
                if slot in gdeltRecord[2]:
                    hdfs.log(LOG_PATH,'Found record with correct date '+str(gdeltRecord),False)
                    timestamp = gdeltRecord[2].split('/')[-1].split('.')[0]
                    if timestamp in checkPointList:
                        hdfs.log(LOG_PATH, 'Found in checkpoints'+str(gdeltRecord),False)
                    else:
                        taskList.append(gdeltRecord)
                        counter= counter+1
    hdfs.log(LOG_PATH,'#'+str(counter)+' tasks created.',False)
    return taskList


def generateDirectoriesTree(RUN_CONTROL_DATE, DIR, SUBDIRS):
    DIR = DIR.replace('{DATE}',RUN_CONTROL_DATE)
    hdfs.mkdir(DIR)
    for subdir in SUBDIRS:
        hdfs.mkdir(DIR + '/' + subdir)

def enqueueTasks(TASK_LIST,LIST_NAME):
    que = redis.Redis(host=REDIS_URL,port=6379)
    hdfs.log(LOG_PATH,'Connected to Redis',False)
    que.delete(LIST_NAME)
    for task in TASK_LIST:
        que.lpush(LIST_NAME,str(task))
        hdfs.log(LOG_PATH,'LeftPushed '+str(task)+' into '+LIST_NAME+' list',False)
    que.client_kill_filter(_id=que.client_id())
    hdfs.log(LOG_PATH,'Disconnected from Redis',False)

if not hdfs.exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = hdfs.readFileAsString(RUN_CONTROL_PATH)
if DATE.endswith('\n'):
    DATE = DATE[:-1]

LOG_PATH = LOG_PATH.replace('{DATE}',DATE)
hdfs.mkdir(LOG_PATH)
LOG_PATH = LOG_PATH +'/extraction-csv.log'
if not hdfs.exists(LOG_PATH):
    hdfs.touch(LOG_PATH)
generateDirectoriesTree(DATE, DATA_DIR,DATA_SUBDIRS)
generateDirectoriesTree(DATE,DB_DIR,[])

if len(sys.argv) > 1:
    print("Max hour {}".format(sys.argv[1]))
    NEW_TASKS = getNewTasksList(DATE, CHECKPOINT_PATH, MASTREFILE_URL, sys.argv[1])
else:
    print("Max hour {}".format(24))
    NEW_TASKS = getNewTasksList(DATE, CHECKPOINT_PATH, MASTREFILE_URL)

enqueueTasks(NEW_TASKS, QUE_NAME)
