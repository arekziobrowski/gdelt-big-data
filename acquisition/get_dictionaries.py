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
TYPES = ['type','knowngroup','ethnic','religion','eventcodes']
hdfsPath = '/data/gdelt/'+str(DATE)+'/cameo/'
for type in TYPES:
    path = hdfsPath+FILE_NAME.replace('{type}',type)
    if hdfs.exists(path):
        continue
    cameoResponse = urllib2.urlopen(URL.replace('{type}',type))
    cameoContent = cameoResponse.read()
    hdfs.write(path, cameoContent)

def getCountries():
    URL = 'https://raw.githubusercontent.com/mysociety/gaze/master/data/fips-10-4-to-iso-country-codes.csv'
    path = hdfsPath+FILE_NAME.replace('{type}','country')
    if hdfs.exists(path):
        return
    cameoResponse = urllib2.urlopen(URL)
    cameoContent = cameoResponse.read()
    countries = []
    for line in cameoContent.split('\n')[1:]:
        splitted = line.split(',')
        if len(splitted) == 3:
            countries.append(splitted[0]+'\t'+splitted[2])
    hdfs.write(path, '\n'.join(countries))

getCountries()