import os

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


def create_etl_hierarchy():
    if not os.path.exists(BASE_PATH):
        os.mkdir(BASE_PATH)

    if not os.path.exists(STAGING_DIR):
        os.mkdir(STAGING_DIR)

    if not os.path.exists(CLEANSED_DIR):
        os.mkdir(CLEANSED_DIR)

    if not os.path.exists(LOAD_DIR):
        os.mkdir(LOAD_DIR)

    for rcd in RUN_CONTROL_DATE_LIST:
        cleansed_rcd = os.path.join(CLEANSED_DIR, rcd)
        if not os.path.exists(cleansed_rcd):
            os.mkdir(cleansed_rcd)

        load_rcd = os.path.join(LOAD_DIR, rcd)
        if not os.path.exists(load_rcd):
            os.mkdir(load_rcd)


def create_etl_cleansed_files():
    for rcd in RUN_CONTROL_DATE_LIST:
        cleansed_rcd = os.path.join(CLEANSED_DIR, rcd)

        with open(os.path.join(cleansed_rcd, ARTICLE_LOOKUP_DAT), 'w'):
            pass
        with open(os.path.join(cleansed_rcd, ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_DAT), 'w'):
            pass
        with open(os.path.join(cleansed_rcd, ARTICLES_API_INFO_CLEANSED_PART_INTERVAL_REJECT), 'w'):
            pass
        with open(os.path.join(cleansed_rcd, ARTICLES_API_INFO_CLEANSED_DAT), 'w'):
            pass

        for time in SAMPLE_TIME_SLOTS_LIST:

            timeslot_dat = ARTICLES_DATA_CLEANSED_PART_TIME_DAT
            timeslot_dat = timeslot_dat.replace(r"_date_", rcd)
            timeslot_dat = timeslot_dat.replace(r"_time_", time)
            with open(os.path.join(cleansed_rcd, timeslot_dat), 'w'):
                pass

            timeslot_reject = ARTICLES_DATA_CLEANSED_PART_TIME_REJECT
            timeslot_reject = timeslot_reject.replace(r"_date_", rcd)
            timeslot_reject = timeslot_reject.replace(r"_time_", time)
            with open(os.path.join(cleansed_rcd, timeslot_reject), 'w'):
                pass

        with open(os.path.join(cleansed_rcd, ARCITLES_DATA_CLEANSED_DAT), 'w'):
            pass


def create_etl_load_files():
    for rcd in RUN_CONTROL_DATE_LIST:
        load_rcd = os.path.join(LOAD_DIR, rcd)

        with open(os.path.join(load_rcd, ARTICLE_DAT), 'w'):
            pass
        with open(os.path.join(load_rcd, ARTICLE_LOOKUP_DAT), 'w'):
            pass
        with open(os.path.join(load_rcd, COLOR_METADATA_DAT), 'w'):
            pass
        with open(os.path.join(load_rcd, COUNTRY_DAT), 'w'):
            pass
        with open(os.path.join(load_rcd, IMAGE_DAT), 'w'):
            pass
        with open(os.path.join(load_rcd, IMAGE_METADATA), 'w'):
            pass
        with open(os.path.join(load_rcd, KEYWORD_METADATA), 'w'):
            pass

# files get overwritten


def create_etl_files():
    create_etl_cleansed_files()
    create_etl_load_files()


def populate_etl():
    pass


def main():
    create_etl_hierarchy()
    create_etl_files()
    populate_etl()


main()
