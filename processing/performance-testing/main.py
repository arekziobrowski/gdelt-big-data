import subprocess
import timeit
import datetime
import os

from config import Config
from hdfs_utils import delete_recursively, write, listPath

RUN_CONTROL_DATE_PATH = '/tech/RUN_CONTROL_DATE.dat'
RUN_CONTROL_DATE_PLACEHOLDER = '{RUN_CONTROL_DATE}'

DICTIONARIES_PATH = '/data/gdelt/{RUN_CONTROL_DATE}/cameo/'
DICTIONARIES_PATH = DICTIONARIES_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

# HDFS
ARTICLE_INFO_JSON = '/data/gdelt/{RUN_CONTROL_DATE}/api/'
ARTICLE_INFO_JSON = ARTICLE_INFO_JSON.replace(
    RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

ARTICLE_CSV = '/data/gdelt/{RUN_CONTROL_DATE}/csv/'
ARTICLE_CSV = ARTICLE_CSV.replace(
    RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

DATA_GDELT_RUN_CONTROL_DATE = '/data/gdelt/{RUN_CONTROL_DATE}/'
DATA_GDELT_RUN_CONTROL_DATE = DATA_GDELT_RUN_CONTROL_DATE.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

TECH_EXTRACTION = '/tech/extraction/{RUN_CONTROL_DATE}/'
TECH_EXTRACTION = TECH_EXTRACTION.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

ETL_STAGING_CLEANSED = '/etl/staging/cleansed/{RUN_CONTROL_DATE}/'
ETL_STAGING_CLEANSED = ETL_STAGING_CLEANSED.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)

ETL_STAGING_LOAD = '/etl/staging/load/{RUN_CONTROL_DATE}/'
ETL_STAGING_LOAD = ETL_STAGING_LOAD.replace(RUN_CONTROL_DATE_PLACEHOLDER, Config.RUN_CONTROL_DATE)


def set_up(max_hour):
    print("Setting up {}".format(RUN_CONTROL_DATE_PATH))
    delete_recursively(RUN_CONTROL_DATE_PATH)
    write(RUN_CONTROL_DATE_PATH, Config.RUN_CONTROL_DATE)

    print("Setting up dictionaries {}".format(DICTIONARIES_PATH))
    subprocess.check_output('python2.7 ../../acquisition/get_dictionaries.py', shell=True)

    # download api
    print("Setting up api {} for hour time slot [0, {}]".format(ARTICLE_INFO_JSON, max_hour))
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

    # csv distinct
    print("Running csv-distinct")
    subprocess.check_output('../run_processing.sh distinct ../csvdistinct/target/csv-distinct-1.0-SNAPSHOT.jar',
                            shell=True)

    # map country
    print("Running country-mapping")
    subprocess.check_output('../run_processing.sh country ../country-mapping/target/country-mapping-1.0-SNAPSHOT.jar',
                            shell=True)

    # # rake
    # print("Running rake")
    # subprocess.check_output('../run_processing.sh rake ../country-mapping/target/rake-1.0-SNAPSHOT.jar',
    #                         shell=True)
    #
    # # image
    # print("Running image-processing")
    # subprocess.check_output(
    #     '../run_processing.sh image ../image-processing/target/image-processing-1.0-SNAPSHOT-jar-with-dependencies.jar',
    #     shell=True)


def tear_down():
    print("Deleting {}".format(RUN_CONTROL_DATE_PATH))
    delete_recursively(RUN_CONTROL_DATE_PATH)

    print("Deleting dictionaries {}".format(DICTIONARIES_PATH))
    delete_recursively(DICTIONARIES_PATH)

    print("Deleting {}".format(DATA_GDELT_RUN_CONTROL_DATE))
    delete_recursively(DATA_GDELT_RUN_CONTROL_DATE)

    print("Deleting {}".format(TECH_EXTRACTION))
    delete_recursively(TECH_EXTRACTION)

    print("Deleting {}".format(ETL_STAGING_CLEANSED))
    delete_recursively(ETL_STAGING_CLEANSED)

    print("Deleting {}".format(ETL_STAGING_LOAD))
    delete_recursively(ETL_STAGING_LOAD)


def test_csv_cleansing():
    print("Running csv-clean-up")

    start_time = timeit.default_timer()
    subprocess.check_output(
        '../run_processing.sh csv ../csvcleanup/target/csv-clean-up-1.0-SNAPSHOT.jar',
        shell=True)
    elapsed = timeit.default_timer() - start_time

    return elapsed


def test_json_cleansing():
    print("Running articleinfo-cleanup")

    start_time = timeit.default_timer()
    subprocess.check_output(
        '../run_processing.sh json ../articleinfo-cleanup/target/articleinfo-cleanup-1.0-SNAPSHOT-jar-with-dependencies.jar',
        shell=True)
    elapsed = timeit.default_timer() - start_time

    return elapsed


def test_article_transformation():
    print("Running article-mapping")

    start_time = timeit.default_timer()
    subprocess.check_output(
        '../run_processing.sh article ../article-mapping/target/article-mapping-1.0-SNAPSHOT.jar',
        shell=True)
    elapsed = timeit.default_timer() - start_time

    return elapsed


def save_to_file(list):
    with open('results.txt', 'a') as results_file:
        results_file.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")

        results_file.write("hours\\test\t")
        results_file.write("csv_cleansing\t")
        results_file.write("json_cleansing\t")
        results_file.write("article_transformation\t")
        results_file.write("\n")

        for results_for_hour in list:
            for one_result in results_for_hour:
                results_file.write("{}\t".format(str(one_result)))
            results_file.write("\n")

        results_file.write("\n")


### main
print("RUN_CONTROL_DATE: {}".format(Config.RUN_CONTROL_DATE))
print("Time ranges: {}".format(Config.HOUR_RANGES))

results = []

tear_down()

for max_hour in Config.HOUR_RANGES:
    set_up(max_hour)

    result_csv_cleansing = test_csv_cleansing()
    result_json_cleansing = test_json_cleansing()
    result_article_transformation = test_article_transformation()

    results.append([max_hour, result_csv_cleansing, result_json_cleansing, result_article_transformation])

    tear_down()

print()
print("RESULTS in seconds")
print(results)

save_to_file(results)
