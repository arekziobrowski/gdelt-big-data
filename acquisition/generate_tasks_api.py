# Script schedules API download tasks into redis queue
# and creates directory/file hierarchy for an acquisition for a RUN_CONTROL_DATE.
#
# Preconditions - tasks to be scheduled are selected based on:
#  - /tech/RUN_CONTROL_DATE.dat file
#  - /tech/extraction/{RUN_CONTROL_DATE}/checkpoint/CHECKPOINT-{RUN_CONTROL_DATE}.checkpoint file
#
# Usage:
# python2.7 generate_tasks_api.py

import hdfs_utils as hdfs
import urlparse
import redis


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
API_EXTRACTION_LOG_DIR = API_EXTRACTION_LOG_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
API_EXTRACTION_LOG_FILE = API_EXTRACTION_LOG_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)

# checkpoints
API_EXTRACTION_CHECKPOINT_DIR = '/tech/extraction/{RUN_CONTROL_DATE}/checkpoint'
API_EXTRACTION_CHECKPOINT_FILE = '/tech/extraction/{RUN_CONTROL_DATE}/checkpoint/CHECKPOINT-API-{RUN_CONTROL_DATE}.checkpoint'
API_EXTRACTION_CHECKPOINT_DIR = API_EXTRACTION_CHECKPOINT_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
API_EXTRACTION_CHECKPOINT_FILE = API_EXTRACTION_CHECKPOINT_FILE.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)

# hdfs
HDFS_INTERVAL_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/'
IMAGES_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/images/'
TEXTS_DIR = '/data/gdelt/{RUN_CONTROL_DATE}/api/{INTERVAL}/texts/'

HDFS_INTERVAL_DIR = HDFS_INTERVAL_DIR.replace(
    '{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
IMAGES_DIR = IMAGES_DIR.replace('{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)
TEXTS_DIR = TEXTS_DIR.replace('{RUN_CONTROL_DATE}', RUN_CONTROL_DATE)


def generate_daily_time_slots():
    base_slots = []
    for hour in [str(h).zfill(2) for h in range(0, 24)]:
        for minutes in ["00", "30"]:
            time = "{0}{1}{2}".format(hour, minutes, "00")
            base_slots.append(RUN_CONTROL_DATE + time)

    slots = []
    for i in range(0, int(len(base_slots))):
        slots.append([base_slots[i], base_slots[(i+1) % (len(base_slots))]])

    slots[-1][1] = "{0}{1}".format(RUN_CONTROL_DATE, "240000")

    return slots


def get_processed_time_slots_from_checkpoint():
    processed_slots = list()
    checkpoint_file_content = hdfs.readFileAsString(
        API_EXTRACTION_CHECKPOINT_FILE)

    for line in [x.strip() for x in checkpoint_file_content.split("\n")][1:]:
        if line == "":
            continue

        url = line.split("|")[0]
        hdfs_file = line.split("|")[2]

        print(url)
        print(hdfs_file)

        parsed_url = urlparse.urlparse(url)

        start_datetime = urlparse.parse_qs(parsed_url.query)[
            'STARTDATETIME'][0]
        end_datetime = urlparse.parse_qs(parsed_url.query)['ENDDATETIME'][0]

        if hdfs.exists(hdfs_file):
            # file also has to exist
            processed_slots.append([start_datetime, end_datetime])

    return processed_slots


def get_new_tasks_list():
    time_slots = generate_daily_time_slots()
    task_list = []

    already_processed_time_slots = get_processed_time_slots_from_checkpoint()

    for slot in time_slots:
        INTERVAL = slot[0][-6:] + "-" + slot[1][-6:]

        # images
        id = IMAGES_DIR.replace('{INTERVAL}', INTERVAL)
        if not hdfs.exists(id):
            hdfs.mkdir(id)

        # texts
        td = TEXTS_DIR.replace('{INTERVAL}', INTERVAL)
        if not hdfs.exists(td):
            hdfs.mkdir(td)

        if slot not in already_processed_time_slots:
            url = API_URL.replace('{START_DATETIME}', slot[0])
            url = url.replace('{END_DATETIME}', slot[1])
            task_list.append(url)
        else:
            print("Omitting previously processed slot: {0}".format(slot))

    return task_list


def enqueue_tasks(task_list):
    que = redis.Redis(host=REDIS_URL, port=6379)

    hdfs.log(API_EXTRACTION_LOG_FILE, 'Connected to Redis', False)

    for task in task_list:
        que.lpush(QUE_NAME, str(task))
        hdfs.log(API_EXTRACTION_LOG_FILE, 'LeftPushed ' +
                 str(task)+' into ' + QUE_NAME + ' list', False)

    que.client_kill_filter(_id=que.client_id())

    hdfs.log(API_EXTRACTION_LOG_FILE, 'Disconnected from Redis', False)


# log
if not hdfs.exists(API_EXTRACTION_LOG_DIR):
    hdfs.mkdir(API_EXTRACTION_LOG_DIR)
if not hdfs.exists(API_EXTRACTION_LOG_FILE):
    hdfs.touch(API_EXTRACTION_LOG_FILE)

# checkpoint
if not hdfs.exists(API_EXTRACTION_CHECKPOINT_DIR):
    hdfs.mkdir(API_EXTRACTION_CHECKPOINT_DIR)
if not hdfs.exists(API_EXTRACTION_CHECKPOINT_FILE):
    hdfs.write(API_EXTRACTION_CHECKPOINT_FILE,
               'API_URL|FINISH_DATETIME|FILE_LOCATION')


new_tasks = get_new_tasks_list()

enqueue_tasks(new_tasks)
