# Script downloads API data for given time slots stored in redis queue, puts in coresponding
# directories and writes to checkpoint/log files.
#
# INTERVAL parameter is start time and end time (HHMMSS) connected by a dash.
# Example: 120000-123000


import os
import redis
import urlparse
import hdfs_utils as hdfs
import urllib2
import datetime


# redis
REDIS_URL = 'redis-tasks'
QUE_NAME = 'API_DOWNLOAD'

# RUN_CONTROL_DATE
RUN_CONTROL_DATE_FILE = '/tech/RUN_CONTROL_DATE.dat'

if not hdfs.exists(RUN_CONTROL_DATE_FILE):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_DATE_FILE))
RUN_CONTROL_DATE = hdfs.readFileAsString(RUN_CONTROL_DATE_FILE)
if RUN_CONTROL_DATE.endswith('\n'):
    RUN_CONTROL_DATE = RUN_CONTROL_DATE[:-1]

# logs
API_EXTRACTION_LOG_FILE = '/tech/extraction/{RUN_CONTROL_DATE}/log/extraction-api.log'
API_EXTRACTION_LOG_FILE = API_EXTRACTION_LOG_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)

# checkpoints
API_EXTRACTION_CHECKPOINT_FILE = '/tech/extraction/{RUN_CONTROL_DATE}/checkpoint/CHECKPOINT-API-{RUN_CONTROL_DATE}.checkpoint'
API_EXTRACTION_CHECKPOINT_FILE = API_EXTRACTION_CHECKPOINT_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)

# HDFS
ARTICLE_INFO_JSON = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/article_info.json'

ARTICLE_INFO_JSON = ARTICLE_INFO_JSON.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)


def parse_task(task):
    task_list = []
    for quoted_task in task[1][:].split(', '):
        task_list.append(quoted_task[:])
    return task_list


def handle_task(task, run_control_date):
    print("HANDLING: " + str(task))

    try:
        url = task[0]
        parsed_url = urlparse.urlparse(url)

        start_datetime = urlparse.parse_qs(parsed_url.query)[
            'STARTDATETIME'][0]
        start_time = start_datetime[-6:]
        end_datetime = urlparse.parse_qs(parsed_url.query)['ENDDATETIME'][0]
        end_time = end_datetime[-6:]

        response = urllib2.urlopen(url)
        json_content = response.read()

        article_info_json = ARTICLE_INFO_JSON.replace(
            '{INTERVAL}', start_time + "-" + end_time)

        hdfs.write(article_info_json, json_content)
    except Exception as e:
        print(e)
        hdfs.log(API_EXTRACTION_LOG_FILE,
                 'Error {0} while working on task {1}'.format(e, task), False)
        return

    hdfs.append(API_EXTRACTION_CHECKPOINT_FILE, '{}|{}|{}'.format(
        url,
        datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
        article_info_json))


que = redis.Redis(host=REDIS_URL, port=6379)

is_empty = False

while not is_empty:
    task = que.blpop(QUE_NAME, timeout=1)
    if task == None:
        is_empty = True
        print("EMPTY QUEUE")
    else:
        handle_task(parse_task(task), RUN_CONTROL_DATE)

que.client_kill_filter(_id=que.client_id())
