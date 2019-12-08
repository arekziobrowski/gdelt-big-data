from hdfs import InsecureClient
import datetime

client = InsecureClient('http://localhost:14000')

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
    print('APPEND: '+path)
    with client.write(path,append=True) as writer:
        writer.write(data+'\n')

def write(path,data):
    print('WRITE: '+path)
    with client.write(path) as writer:
        writer.write(data+'\n')

def readFileAsString(path):
    with client.read(path) as reader:
        return reader.read()

def listPath(path):
    return client.list(path)

def log(path,content,isError):
    prefix = ''
    if isError:
        prefix = '[ERROR]'
    else:
        prefix = '[INFO]'
    log = prefix+'['+str(datetime.datetime.now())+'] '+content
    print('LOG: '+ log)
    append(path,log)