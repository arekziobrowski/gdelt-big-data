import hdfs_utils as hdfs
import urllib2
import redis

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'
QUE_NAME = 'CSV_LIST'
REDIS_URL = 'redis-tasks'
RELIGION_DICT_PATH = '/data/gdelt/{DATE}/cameo/CAMEO.religion.txt'
MASTREFILE_URL = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'

RELIGION_DICTIONARY = u'''CODE\tLABEL\r
ADR\tAfrican Diasporic Religion\r
ALE\tAlewi\r
ATH\tAgnostic\r
BAH\tBahai Faith\r
BUD\tBuddhism\r
CHR\tChristianity\r
CON\tConfucianism\r
CPT\tCoptic\r
CTH\tCatholic\r
DOX\tOrthodox\r
DRZ\tDruze\r
HIN\tHinduism\r
HSD\tHasidic\r
ITR\tIndigenous Tribal Religion\r
JAN\tJainism\r
JEW\tJudaism\r
JHW\tJehovah's Witness\r
LDS\tLatter Day Saints\r
MOS\tMuslim\r
MRN\tMaronite\r
NRM\tNew Religious Movement\r
PAG\tPagan\r
PRO\tProtestant\r
SFI\tSufi\r
SHI\tShia\r
SHN\tOld Shinto School\r
SIK\tSikh\r
SUN\tSunni\r
TAO\tTaoist\r
UDX\tUltra-Orthodox\r
ZRO\tZoroastrianism\r

'''

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

def getCsvCount(path,date,suffix):
    counter = 0
    for file in hdfs.listPath(path):
        if file.startswith(date) and file.endswith(suffix):
            counter = counter+1
    return counter

if not hdfs.exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = hdfs.readFileAsString(RUN_CONTROL_PATH)
if DATE.endswith('\n'):
    DATE = DATE[:-1]

if not getTaskCount() == 0:
    print("Taskqueue is not empty!")
else:
    print("Taskqueue\t\tOK")

masterFileCount = getMasterFileCount(DATE)
csvCount = getCsvCount('/data/gdelt/'+DATE+'/csv/',DATE,'.export.csv')
if not csvCount == masterFileCount:
    print("Not enough CSV files. There should be {} files inside /data/gdelt/{}/csv/ dir instead of {}.".format(masterFileCount,DATE,csvCount))
else:
    print("#CSV files\t\tOK")

religionLinesHDFS = hdfs.readFileAsString(RELIGION_DICT_PATH.replace('{DATE}',DATE)).split('\n')
religionLines = RELIGION_DICTIONARY.split('\n')
if not len(religionLinesHDFS) == len(religionLines):
    print("Diffrent lengths of RELIGION DICTIONARY (HDFS_FILE: {},HARDCODED_FILE: {})".format(len(religionLinesHDFS),len(religionLines)))
else:
    linesLen = len(religionLines)
    iterator = 0
    for iterator in range(0,linesLen):
        if not religionLines[iterator].encode('ascii') == religionLinesHDFS[iterator].encode('ascii'):
            print("Diffrence detected:")
            print("HDFS file:\t\t'"+religionLinesHDFS[iterator].rstrip()+"'")
            print("Hardcoded file:\t\t'"+religionLines[iterator].rstrip()+"'")
        else:
            print("Line "+str(iterator)+"\t\tOK")

exampleCSVFile = hdfs.listPath('/data/gdelt/'+DATE+'/csv/')[0]
exampleCSVContent = hdfs.readFileAsString('/data/gdelt/'+DATE+'/csv/'+exampleCSVFile)
csvFormatError = False
lineCounter = 0
for line in exampleCSVContent.split('\n'):
    if line != '' and not len(line.split('\t')) == 61:
        csvFormatError=True
        print("Line #{} contains {} columns.\nLine: '{}'".format(lineCounter, len(line.split('\t'))+1,line))
    lineCounter = lineCounter + 1
if not csvFormatError:
    print("Example CSV file\t\tOK")