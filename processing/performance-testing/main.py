import subprocess

from config import Config
from hdfs_utils import delete_recursively, write, listPath

RUN_CONTROL_DATE_PATH = '/tech/RUN_CONTROL_DATE.dat'
RUN_CONTROL_DATE_PLACEHOLDER = '{RUN_CONTROL_DATE}'

DICTIONARIES_PATH = '/data/gdelt/{RUN_CONTROL_DATE}/cameo/'
DICTIONARIES_PATH = DICTIONARIES_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.run_control_date)

# HDFS
ARTICLE_INFO_JSON = '/data/gdelt/{RUN_CONTROL_DATE}/api/'
ARTICLE_INFO_JSON = ARTICLE_INFO_JSON.replace(
    RUN_CONTROL_DATE_PLACEHOLDER, Config.run_control_date)

ARTICLE_CSV = '/data/gdelt/{RUN_CONTROL_DATE}/csv/'
ARTICLE_CSV = ARTICLE_CSV.replace(
    RUN_CONTROL_DATE_PLACEHOLDER, Config.run_control_date)

DATA_GDELT_RUN_CONTROL_DATE = '/data/gdelt/{RUN_CONTROL_DATE}/'
DATA_GDELT_RUN_CONTROL_DATE = DATA_GDELT_RUN_CONTROL_DATE.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.run_control_date)

TECH_EXTRACTION = '/tech/extraction/{RUN_CONTROL_DATE}/'
TECH_EXTRACTION = TECH_EXTRACTION.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.run_control_date)


def set_up(max_hour):
    print("Setting up {}".format(RUN_CONTROL_DATE_PATH))
    delete_recursively(RUN_CONTROL_DATE_PATH)
    write(RUN_CONTROL_DATE_PATH, Config.run_control_date)

    print("Setting up dictionaries {}".format(DICTIONARIES_PATH))
    subprocess.check_output('python2.7 ../../acquisition/get_dictionaries.py', shell=True)

    # download api
    print("Setting up api {} for time slot [0, {}]".format(ARTICLE_INFO_JSON, max_hour))
    subprocess.check_output('python2.7 ../../acquisition/generate_tasks_api.py {}'.format(max_hour),
                            shell=True)
    subprocess.check_output('python2.7 ../../acquisition/download_api.py', shell=True)
    for path in listPath(ARTICLE_INFO_JSON):
        print("\t {}".format(path))

    # download csv
    print("Setting up csv {} for hour time slot [0, {}]".format(ARTICLE_CSV, max_hour))
    subprocess.check_output('python2.7 ../../acquisition/generate_tasks.py {}'.format(max_hour),
                            shell=True)
    subprocess.check_output('python2.7 ../../acquisition/download_csv.py', shell=True)
    for path in listPath(ARTICLE_CSV):
        print("\t {}".format(path))


def tear_down():
    print("Deleting {}".format(RUN_CONTROL_DATE_PATH))
    delete_recursively(RUN_CONTROL_DATE_PATH)

    print("Deleting dictionaries {}".format(DICTIONARIES_PATH))
    delete_recursively(DICTIONARIES_PATH)

    print("Deleting {}".format(DATA_GDELT_RUN_CONTROL_DATE))
    delete_recursively(DATA_GDELT_RUN_CONTROL_DATE)

    print("Deleting {}".format(TECH_EXTRACTION))
    delete_recursively(TECH_EXTRACTION)

    # delete load


def test():
    pass


# main
print("RUN_CONTROL_DATE: {}".format(Config.run_control_date))

# hour_ranges = [Config.time_windows_small, Config.time_window_medium, Config.time_windows_big]
hour_ranges = [Config.time_windows_small, ]
print("Time ranges: {}".format(hour_ranges))

for max_hour in hour_ranges:
    set_up(max_hour)

    test()

    tear_down()
