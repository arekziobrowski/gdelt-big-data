import os
import subprocess
# import avro

RUN_CONTROL_DATE_LIST = [r"20191123", ]
SAMPLE_TIME_SLOTS_LIST = [r"120000", r"121500", r"123000"]

BASE_PATH = r"/etl"
STAGING_DIR = os.path.join(BASE_PATH, r"staging")
CLEANSED_DIR = os.path.join(STAGING_DIR, r"cleansed")
LOAD_DIR = os.path.join(STAGING_DIR, r"load")

# cleansed
ARTICLE_LOOKUP_DAT = r"article_lookup.dat"
ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_DAT = r"articles-api-info-cleansed-part-INTERVAL.dat"
ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_REJECT = r"articles-api-info-cleansed-part-INTERVAL.reject"
ARTICLES_API_INFO_CLEANSED_DAT = r"articles-api-info-cleansed.dat"
ARTICLES_DATA_CLEANSED_PART_TIME_DAT = r"articles-data-cleansed-part-_date__time_.dat"
ARTICLES_DATA_CLEANSED_PART_TIME_REJECT = r"articles-data-cleansed-part-_date__time_.reject"
ARCITLES_DATA_CLEANSED_DAT = r"articles-data-cleansed.dat"

# load
ARTICLE_DAT = r"article.dat"
ARTICLE_LOOKUP_DAT = r"article_lookup.dat"
COLOR_METADATA_DAT = r"color_metadata.dat"
COUNTRY_DAT = r"country.dat"
IMAGE_DAT = r"image.dat"
IMAGE_METADATA = r"image_metadata.dat"
KEYWORD_METADATA = r"keyword_metadata.dat"


EMPTY_FILE = "tmp"


def dir_exists_hdfs(path):
    output = subprocess.Popen(
        ['hdfs', 'dfs', '-test', '-d', path], stdout=subprocess.PIPE).communicate()[0]
    if output == 0:
        return True
    return False


def mkdir_hdfs(path):
    subprocess.Popen(['hdfs', 'dfs', '-mkdir', '-p', path],
                     stdout=subprocess.PIPE).communicate()[0]


def rm_dir_hdfs(path):
    subprocess.Popen(['hdfs', 'dfs', '-rm', '-R', path, '2>',
                      '/dev/null'], stdout=subprocess.PIPE).communicate()[0]


def append_to_file_hdfs(source_file, dest_path):
    subprocess.Popen(['hdfs', 'dfs', '-appendToFile', source_file,
                      dest_path], stdout=subprocess.PIPE).communicate()[0]


def create_etl_hierarchy():
    if not dir_exists_hdfs(BASE_PATH):
        mkdir_hdfs(BASE_PATH)

    if not dir_exists_hdfs(STAGING_DIR):
        mkdir_hdfs(STAGING_DIR)

    if not dir_exists_hdfs(CLEANSED_DIR):
        mkdir_hdfs(CLEANSED_DIR)

    if not dir_exists_hdfs(LOAD_DIR):
        mkdir_hdfs(LOAD_DIR)

    for rcd in RUN_CONTROL_DATE_LIST:
        cleansed_rcd = os.path.join(CLEANSED_DIR, rcd)
        if not dir_exists_hdfs(cleansed_rcd):
            mkdir_hdfs(cleansed_rcd)

        load_rcd = os.path.join(LOAD_DIR, rcd)
        if not dir_exists_hdfs(load_rcd):
            mkdir_hdfs(load_rcd)


def create_etl_cleansed_files():
    for rcd in RUN_CONTROL_DATE_LIST:
        cleansed_rcd = os.path.join(CLEANSED_DIR, rcd)

        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            cleansed_rcd, ARTICLE_LOOKUP_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            cleansed_rcd, ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            cleansed_rcd, ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_REJECT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            cleansed_rcd, ARTICLES_API_INFO_CLEANSED_DAT))

        for time in SAMPLE_TIME_SLOTS_LIST:

            timeslot_dat = ARTICLES_DATA_CLEANSED_PART_TIME_DAT
            timeslot_dat = timeslot_dat.replace(r"_date_", rcd)
            timeslot_dat = timeslot_dat.replace(r"_time_", time)

            append_to_file_hdfs(EMPTY_FILE, os.path.join(
                cleansed_rcd, timeslot_dat))

            timeslot_reject = ARTICLES_DATA_CLEANSED_PART_TIME_REJECT
            timeslot_reject = timeslot_reject.replace(r"_date_", rcd)
            timeslot_reject = timeslot_reject.replace(r"_time_", time)

            append_to_file_hdfs(EMPTY_FILE, os.path.join(
                cleansed_rcd, timeslot_reject))

        append_to_file_hdfs(EMPTY_FILE, os.path.join(cleansed_rcd, ARCITLES_DATA_CLEANSED_DAT))


def create_etl_load_files():
    for rcd in RUN_CONTROL_DATE_LIST:
        load_rcd = os.path.join(LOAD_DIR, rcd)

        append_to_file_hdfs(EMPTY_FILE, os.path.join(load_rcd, ARTICLE_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            load_rcd, ARTICLE_LOOKUP_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            load_rcd, COLOR_METADATA_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(load_rcd, COUNTRY_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(load_rcd, IMAGE_DAT))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(load_rcd, IMAGE_METADATA))
        append_to_file_hdfs(EMPTY_FILE, os.path.join(
            load_rcd, KEYWORD_METADATA))


def create_etl_files():
    # files get overwritten!
    create_etl_cleansed_files()
    create_etl_load_files()


def populate_etl():
    pass


def main():
    create_etl_hierarchy()

    with open(EMPTY_FILE, "w") as f:
        f.write("")

    create_etl_files()

    populate_etl()


main()
