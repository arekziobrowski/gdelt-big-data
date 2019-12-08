import hdfs_utils as hdfs
import urllib2

RUN_CONTROL_PATH = '/tech/RUN_CONTROL_DATE.dat'

if not hdfs.exists(RUN_CONTROL_PATH):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_PATH))
DATE = hdfs.readFileAsString(RUN_CONTROL_PATH)
if DATE.endswith('\n'):
    DATE = DATE[:-1]

FILE_NAME = 'CAMEO.{type}.txt'
URL = 'https://www.gdeltproject.org/data/lookups/CAMEO.{type}.txt'
TYPES = ['country','type','knowngroup','ethnic','religion','eventcodes']
hdfsPath = '/data/gdelt/'+str(DATE)+'/cameo/'
for type in TYPES:
    path = hdfsPath+FILE_NAME.replace('{type}',type)
    if hdfs.exists(path):
        continue
    cameoResponse = urllib2.urlopen(URL.replace('{type}',type))
    cameoContent = cameoResponse.read()
    hdfs.write(path, cameoContent)