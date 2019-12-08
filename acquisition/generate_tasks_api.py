import hdfs_utils as hdfs
import urllib2
import datetime
import redis
import os


RUN_CONTROL_DATE_FILE = '/tech/RUN_CONTROL_DATE.dat'

# redis
REDIS_URL = 'redis-tasks'
QUE_NAME = 'API_DOWNLOAD'

# api
API_URL = 'https://api.gdeltproject.org/api/v2/doc/doc?query=sourcelang:english&format=json&STARTDATETIME={START_DATETIME}&ENDDATETIME={END_DATETIME}'

if not hdfs.exists(RUN_CONTROL_DATE_FILE):
    raise Exception('There is not tech file in '+str(RUN_CONTROL_DATE_FILE))
RUN_CONTROL_DATE = hdfs.readFileAsString(RUN_CONTROL_DATE_FILE)
if RUN_CONTROL_DATE.endswith('\n'):
    RUN_CONTROL_DATE = RUN_CONTROL_DATE[:-1]

# logs
API_EXTRACTION_LOG_DIR = '/tech/extraction/{RUN_CONTROL_DATE}/log/'
API_EXTRACTION_LOG_FILE = '/tech/extraction/{RUN_CONTROL_DATE}/log/extraction-api.log'
API_EXTRACTION_CHECKPOINT_DIR = '/tech/extraction/{RUN_CONTROL_DATE}/checkpoint'
API_EXTRACTION_CHECKPOINT_FILE = '/tech/extraction/{RUN_CONTROL_DATE}/checkpoint/CHECKPOINT-{RUN_CONTROL_DATE}.checkpoint'

API_EXTRACTION_LOG_DIR = API_EXTRACTION_LOG_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
API_EXTRACTION_LOG_FILE = API_EXTRACTION_LOG_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
API_EXTRACTION_CHECKPOINT_DIR = API_EXTRACTION_CHECKPOINT_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
API_EXTRACTION_CHECKPOINT_FILE = API_EXTRACTION_CHECKPOINT_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)

# HDFS
HDFS_INTERVAL_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/'
ARTICLE_INFO_JSON = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/article_info.json'
IMAGES_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/images/'
TEXTS_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/texts/'

HDFS_INTERVAL_DIR = HDFS_INTERVAL_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
ARTICLE_INFO_JSON = ARTICLE_INFO_JSON.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
IMAGES_DIR = IMAGES_DIR.replace('{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
TEXTS_DIR = TEXTS_DIR.replace('{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)


def generate_daily_time_slots(RUN_CONTROL_DATE):
    base_slots = []
    for hour in [str(h).zfill(2) for h in range(0, 24)]:
        for minutes in ["00", "30"]:
            time = "{0}{1}{2}".format(hour, minutes, "00")
            base_slots.append(RUN_CONTROL_DATE + time)

    slots = []
    for i in range(0, int(len(base_slots))):
        slots.append([base_slots[i], base_slots[(i+1) % (len(base_slots))]])

    # change last for new date
    slots[-1][1] = "{0}{1}".format(RUN_CONTROL_DATE, "240000")

    return slots


def get_new_tasks_list(run_control_date, checkpoint_path, api_url, images_dir, texts_dir, article_info_json):
    # todo: run from checkpoint

    time_slots = generate_daily_time_slots(run_control_date)
    task_list = []

    for slot in time_slots:
        url = api_url.replace('{START_DATETIME}', slot[0])
        url = url.replace('{END_DATETIME}', slot[1])
        task_list.append(url)

        INTERVAL = slot[0][-6:]+"-"+slot[1][-6:]

        # images
        id = images_dir.replace('{INTERVAL}', INTERVAL)
        if not hdfs.exists(id):
            hdfs.mkdir(id)

        # texts
        td = texts_dir.replace('{INTERVAL}', INTERVAL)
        if not hdfs.exists(td):
            hdfs.mkdir(td)

        # article_info.json
        aij = ARTICLE_INFO_JSON.replace('{INTERVAL}', INTERVAL)
        if not hdfs.exists(aij):
            hdfs.touch(aij)

    return task_list


def enqueue_tasks(task_list, list_name, api_extraction_log_file):
    que = redis.Redis(host=REDIS_URL, port=6379)

    hdfs.log(api_extraction_log_file, 'Connected to Redis', False)

    for task in task_list:
        que.lpush(list_name, str(task))
        hdfs.log(api_extraction_log_file, 'LeftPushed ' +
                 str(task)+' into '+list_name+' list', False)

    que.client_kill_filter(_id=que.client_id())

    hdfs.log(api_extraction_log_file, 'Disconnected from Redis', False)


if not hdfs.exists(RUN_CONTROL_DATE_FILE):
    raise Exception('There is not tech file in ' + str(RUN_CONTROL_DATE_FILE))
RUN_CONTROL_DATE = hdfs.readFileAsString(RUN_CONTROL_DATE_FILE)
if RUN_CONTROL_DATE.endswith('\n'):
    RUN_CONTROL_DATE = RUN_CONTROL_DATE[:-1]

# log
if not hdfs.exists(API_EXTRACTION_LOG_DIR):
    hdfs.mkdir(API_EXTRACTION_LOG_DIR)
if not hdfs.exists(API_EXTRACTION_LOG_FILE):
    hdfs.touch(API_EXTRACTION_LOG_FILE)

# checkpoint
if not hdfs.exists(API_EXTRACTION_CHECKPOINT_DIR):
    hdfs.mkdir(API_EXTRACTION_CHECKPOINT_DIR)
if not hdfs.exists(API_EXTRACTION_CHECKPOINT_FILE):
    hdfs.write(API_EXTRACTION_CHECKPOINT_FILE, 'FINISH_DATE|FILE_LOCATION')


new_tasks = get_new_tasks_list(
    RUN_CONTROL_DATE, API_EXTRACTION_CHECKPOINT_DIR, API_URL, IMAGES_DIR, TEXTS_DIR, ARTICLE_INFO_JSON)

enqueue_tasks(new_tasks, QUE_NAME, API_EXTRACTION_LOG_FILE)
