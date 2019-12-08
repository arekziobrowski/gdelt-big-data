from hdfs import InsecureClient
import urllib2
import datetime
import redis

client = InsecureClient('http://localhost:14000')

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'
REDIS_URL = 'redis-tasks'
CHECKPOINT_PATH = '/tech/extraction/{DATE}/checkpoint/'
MASTREFILE_URL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
DATA_DIR = '/data/gdelt/{DATE}'
DB_DIR = '/data/db/{DATE}'
DATA_SUBDIRS = ['csv','api','cameo','reject']
LOG_PATH = '/tech/acquisition/logs'
QUE_NAME = 'CSV_LIST'

def exists(path):
    return client.status(path,strict=False) != None

def mkdir(path):
    print('MKDIR: '+path)
    client.makedirs(path)

def touch(path):
    print('TOUCH: '+path)
    with client.write(path) as writer:
        writer.write('')

def append(path,data):
    with client.write(path,append=True) as writer:
        writer.write(data+'\n')
    print(path,data)

def readFileAsString(path):
    with client.read(path) as reader:
        return reader.read()

def log(path,content,isError):
    prefix = ''
    if isError:
        prefix = '[ERROR]'
    else:
        prefix = '[INFO]'
    log = prefix+'['+str(datetime.datetime.now())+']'+content
    print('LOG: '+ log)
    append(path,log)

def getCheckpointsList(path,RUN_CONTROL_DATE):
    EXPORT_FILE_SUFFIX = '.export.csv'
    path = path.replace('{DATE}',str(RUN_CONTROL_DATE))
    if not exists(path):
        return []

    checkpointFileContent = readFileAsString(path)
    checkpointList = []
    for line in checkpointFileContent.split('\n')[1:] :
        if line == '':
            continue

        if not line.endswith(EXPORT_FILE_SUFFIX):
            errorMessage = str.format('"%s" does not end with the suffix ".export.csv"',line)
            print(line)
            log(LOG_PATH,errorMessage,True)
        else:
            splitted_line = line.split('/')
            pathDate = splitted_line[3]
            if pathDate == RUN_CONTROL_DATE:
                fileName = splitted_line[5].split('.')[0]
                checkpointList.append(fileName)
    return checkpointList

def getNewTasksList(CONTROL_DATE, CHECKPOINT_PATH, MASTREFILE_URL):
    EXPORT_FILE_SUFFIX_MASTER_FILE = '.export.CSV.zip'

    checkPointList = getCheckpointsList(CHECKPOINT_PATH,CONTROL_DATE)
    masterFileReseponse = urllib2.urlopen(MASTREFILE_URL)
    masterFile = masterFileReseponse.read()
    counter = 0
    taskList = []
    for line in masterFile.split('\n'):
        gdeltRecord = line.split(' ')
        if len(gdeltRecord) != 3:
            log(LOG_PATH,'Invalid record in GDELT MASTER FILE',False)
            continue
        if CONTROL_DATE in gdeltRecord[2] and gdeltRecord[2].endswith(EXPORT_FILE_SUFFIX_MASTER_FILE):
            log(LOG_PATH,'Found record with correct date '+str(gdeltRecord),False)
            timestamp = gdeltRecord[2].split('/')[-1].split('.')[0]
            if timestamp in checkPointList:
                log(LOG_PATH, 'Found in checkpoints'+str(gdeltRecord),False)
            else:
                taskList.append(gdeltRecord)
                counter= counter+1
    log(LOG_PATH,'#'+str(counter)+' tasks created.',False)
    return taskList


def generateDirectoriesTree(RUN_CONTROL_DATE, DIR, SUBDIRS):
    DIR = DIR.replace('{DATE}',RUN_CONTROL_DATE)
    mkdir(DIR)
    for subdir in SUBDIRS:
        mkdir(DIR + '/' + subdir)

def enqueueTasks(TASK_LIST,LIST_NAME):
    que = redis.Redis(host=REDIS_URL,port=6379)
    log(LOG_PATH,'Connected to Redis',False)
    for task in TASK_LIST:
        que.lpush(LIST_NAME,str(task))
        log(LOG_PATH,'LeftPushed '+str(task)+' into '+LIST_NAME+' list',False)
    que.client_kill_filter(_id=que.client_id())
    log(LOG_PATH,'Disconnected from Redis',False)

if not exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = readFileAsString(RUN_CONTROL_PATH)

mkdir(LOG_PATH)
LOG_PATH = LOG_PATH +'/acquisition_logs'
if not exists(LOG_PATH):
    touch(LOG_PATH)
generateDirectoriesTree(DATE, DATA_DIR,DATA_SUBDIRS)
generateDirectoriesTree(DATE,DB_DIR,[])
 
NEW_TASKS = getNewTasksList(DATE, CHECKPOINT_PATH, MASTREFILE_URL)
enqueueTasks(NEW_TASKS, QUE_NAME)
