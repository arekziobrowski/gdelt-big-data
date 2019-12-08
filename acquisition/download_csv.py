import md5
import urllib2
import redis
import datetime
import zipfile
import hdfs_utils as hdfs
from os import remove

LOG_PATH = '/tech/acquisition/log/extraction-csv.log'
REJECT_PATH = '/data/gdelt/{DATE}/reject/csv/'
ACCEPT_PATH = '/data/gdelt/{DATE}/csv/'
CHECKPOINT_PATH = '/tech/extraction/{DATE}/checkpoint/CHECKPOINT-{DATE}.checkpoint'
RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'

def reject(zipContent, fileName,DATE):
    print("REJECT: "+fileName)
    hdfs.log(LOG_PATH,'Reject file "'+fileName+'"',True)
    hdfs.write(REJECT_PATH+fileName,zipContent)

def checkMd5Sum(content, md5sum):
    md5Object = md5.new(content)
    return md5sum == md5Object.hexdigest()

def saveFileAs(content, fileName):
    file = open(fileName,'w')
    file.write(content)
    file.close()

def checkpoint(path, chpoint_value):
    line = str(datetime.datetime.now())+'|'+ str(chpoint_value)
    hdfs.append(path,line)

def readAndPutToHdfs(path,hdfs_path):
    file = open(path,'r')
    fileContent = file.read()
    hdfs.write(hdfs_path,fileContent)
    file.close()

def handleTask(TASK,DATE):
    print("HANDLING: "+TASK)
    ZIP_FILENAME = TASK[2].split('/')[-1]

    zipResponse = urllib2.urlopen(TASK[2])
    zipContent = zipResponse.read()
    if not checkMd5Sum(zipContent,TASK[1]):
        reject(zipContent,ZIP_FILENAME,DATE)
        hdfs.write(REJECT_PATH+ZIP_FILENAME,zipContent)
        return
    else:
        hdfs.log(LOG_PATH,ZIP_FILENAME+' has correct md5Sum value',False)
    
    saveFileAs(zipContent,ZIP_FILENAME)
    print("SAVED: "+ZIP_FILENAME)
    with zipfile.ZipFile(ZIP_FILENAME,'r') as zip_ref:
        zip_ref.extractall('.')
    CSV_FILENAME =  ZIP_FILENAME[0:-4]
    print("UNZIPPED: "+CSV_FILENAME)

    HDFS_PATH = ACCEPT_PATH + CSV_FILENAME[0:-4] + '.csv'
    readAndPutToHdfs(CSV_FILENAME,HDFS_PATH)
    checkpoint(CHECKPOINT_PATH,HDFS_PATH)
    print("CHECKPOINT: "+HDFS_PATH)
    remove(ZIP_FILENAME)
    remove(CSV_FILENAME)
    
if not hdfs.exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = hdfs.readFileAsString(RUN_CONTROL_PATH)

if not hdfs.exists(LOG_PATH):
    hdfs.touch(LOG_PATH)
REJECT_PATH = REJECT_PATH.replace('{DATE}',DATE)
ACCEPT_PATH = ACCEPT_PATH.replace('{DATE}',DATE)
CHECKPOINT_PATH = CHECKPOINT_PATH.replace('{DATE}',DATE)

if not hdfs.exists(CHECKPOINT_PATH):
    hdfs.touch(CHECKPOINT_PATH)

TASK = ['247339', 'c27c74fe937c52c97d865b036dc759d2', 'http://data.gdeltproject.org/gdeltv2/20191207000000.export.CSV.zip']
handleTask(TASK,DATE)