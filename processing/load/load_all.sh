#!/bin/bash

declare -r RUN_CONTROL_DATE_PATH='/tech/RUN_CONTROL_DATE.dat'
hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
if [ $? != 0 ] ; then
    error "[$(date)] No RUN_CONTROL_DATE.dat found!"
    exit 1
fi

declare -r RUN_CONTROL_DATE=$(hdfs dfs -cat $RUN_CONTROL_DATE_PATH)

# ./load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/country.dat* -t country # Comment out only for initial load
./load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/rake -t article_keyword
./load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/article/article.dat* -t article
./load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/image_metadata.dat -t image_metadata
./load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/image.dat -t image
