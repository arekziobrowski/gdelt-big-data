#!/bin/bash

# Script for running whole processing

declare -r RUN_CONTROL_DATE_PATH='/tech/RUN_CONTROL_DATE.dat'
hdfs dfs -test -e $RUN_CONTROL_DATE_PATH
if [ $? != 0 ] ; then
    error "[$(date)] No RUN_CONTROL_DATE.dat found!"
    exit 1
fi

declare -r RUN_CONTROL_DATE=$(hdfs dfs -cat $RUN_CONTROL_DATE_PATH)

./run_processing.sh csv csvcleanup/target/csv-clean-up-1.0-SNAPSHOT.jar
./run_processing.sh json articleinfo-cleanup/target/articleinfo-cleanup-1.0-SNAPSHOT-jar-with-dependencies.jar
./run_processing.sh country country-mapping/target/country-mapping-1.0-SNAPSHOT.jar
./run_processing.sh article article-mapping/target/article-mapping-1.0-SNAPSHOT.jar

#./load/load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/country.dat* -t country # Comment out only for initial load
./load/load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/article/article.dat* -t article
./get_article_lookup.sh

./run_processing.sh image imageprocessing/target/image-processing-1.0-SNAPSHOT-jar-with-dependencies.jar
./load/load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/image_metadata.dat -t image_metadata
./load/load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/image.dat -t image

./run_processing.sh rake rake/target/rake-1.0-SNAPSHOT.jar
./load/load_db.sh -f /etl/staging/load/$RUN_CONTROL_DATE/rake -t article_keyword


