# from hdfs import InsecureClient
import urllib2
import datetime

# client = InsecureClient('http://localhost:14000')

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'
REDIS_URL = 'http://redis-tasks'
CHECKPOINT_PATH = '/tech/extraction/{RC_DATE}/checkpoint/'
MASTREFILE_URL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
EXPORT_FILE_SUFFIX = '.export.csv'
EXPORT_FILE_SUFFIX_MASTER_FILE = '.export.CSV.zip'
LOG_PATH = '';

def log(path,content,isError):
    print('['+str(datetime.datetime.now())+']'+content)

def exists(path):
    client.status(path,strict=False) != None

def readFileAsString(path):
    with client.read(path) as reader:
        return reader.read()

def r1(path):
    f = open("CHECKPOINT-20191111.checkpoint", "r")
    return f.read()

def getCheckpointsList(path,RUN_CONTROL_DATE):
    checkpointFileContent = r1(path) #readFileAsString(path)
    checkpointList = []
    for line in checkpointFileContent.split('\n')[1:] :
        if line == '':
            continue

        if not line.endswith(EXPORT_FILE_SUFFIX):
            errorMessage = str.format('"%s" nie jest zakonczona suffixem ".export.csv"',line)
            print(line)
            log(LOG_PATH,errorMessage,True)
        else:
            splitted_line = line.split('/')
            pathDate = splitted_line[3]
            if pathDate == RUN_CONTROL_DATE:
                fileName = splitted_line[5].split('.')[0]
                checkpointList.append(fileName)
    return checkpointList
    
# if not exists(RUN_CONTROL_PATH):
#     raise Exception('There is not tech file in {}.',RUN_CONTROL_PATH)
# date = readFileAsString(RUN_CONTROL_PATH)
date='201911'
checkPointList = getCheckpointsList(CHECKPOINT_PATH,'201911')
print(checkPointList)
masterFileReseponse = urllib2.urlopen(MASTREFILE_URL)
masterFile = masterFileReseponse.read()
counter = 0
taskList = []
for line in masterFile.split('\n'):
    gdeltRecord = line.split(' ')
    if len(gdeltRecord) != 3:
        log(LOG_PATH,'Invalid record in GDELT MASTER FILE',False)
        continue
    if date in gdeltRecord[2] and gdeltRecord[2].endswith(EXPORT_FILE_SUFFIX_MASTER_FILE):
        log(LOG_PATH,'Found record with correct date '+str(gdeltRecord),False)
        timestamp = gdeltRecord[2].split('/')[-1].split('.')[0]
        if timestamp in checkPointList:
            log(LOG_PATH, 'Found in checkpoints'+str(gdeltRecord),False)
        else:
            taskList.append(gdeltRecord)

print(taskList)
