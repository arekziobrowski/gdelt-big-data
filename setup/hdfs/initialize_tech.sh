#!/bin/bash

# Script for creating and populating /tech HDFS directory structure

declare -r BOLD="\033[1m"
declare -r RESET="\033[0m"
declare -r RED="\033[31m"
declare -r LIGHT_BLUE='\033[1;34m'
declare -r GREEN='\033[32m'

declare -r EXTRACTION_PATH='/tech/extraction'
declare -r TRANSFORMATION_PATH='/tech/transformation'
declare -r LOAD_PATH='/tech/load'

function error() {
    echo -e "${RED}${BOLD}${*}${RESET}" >&2
}

function success() {
    echo -e "${GREEN}${*}${RESET}" >&1
}

function help() {
    echo -e "${BOLD}Create and populate /tech HDFS directory structure. ${RESET}"
    echo -e "${BOLD}Usage${RESET}: ./initialize_tech"
    echo -e "\t-r\tReplace the existing directories"
    echo -e "\t-p\tPopulate the created directories"
    echo -e "\t-h\tPrints this help message"
}

# Creates a directory
#   $1 - directory to be created
#   $2 - directory removal indicator
function create_directory() {
    if [ ! -z "$2" ] ; then
        hdfs dfs -rm -R $1 2> /dev/null
    fi

    hdfs dfs -test -d $1
    if [ $? != 0 ] ; then
        echo "Creating $1"
        hdfs dfs -mkdir -p $1
    else
        error "Directory $1 already exists!"
    fi
}

function main() {
    create_directory $EXTRACTION_PATH/log $REPLACE
    create_directory $EXTRACTION_PATH/checkpoint $REPLACE
    create_directory $TRANSFORMATION_PATH/log $REPLACE
    create_directory $LOAD_PATH/log $REPLACE

    if [ ! -z "$POPULATE" ] ; then
        echo "Populating directories"

        printf "20191112" | hdfs dfs -appendToFile - /tech/RUN_CONTROL_DATE.dat

        create_directory $EXTRACTION_PATH/log/20191112
        echo "FINISH_DATE|FILE_LOCATION" | hdfs dfs -appendToFile - $EXTRACTION_PATH/checkpoint/20191112/CHECKPOINT-20191112.checkpoint
        echo "2019-11-12 12:45:40|/data/gdelt/201911/csv/20191111000000.export.csv" | hdfs dfs -appendToFile - $EXTRACTION_PATH/checkpoint/20191112/CHECKPOINT-20191112.checkpoint
        echo "2019-11-12 12:45:40|/data/gdelt/201911/csv/20191111001500.export.csv" | hdfs dfs -appendToFile - $EXTRACTION_PATH/checkpoint/20191112/CHECKPOINT-20191112.checkpoint
    fi

    success 'Finished!'
}

while getopts ":hrp" OPT; do
    case ${OPT} in
        h)
            help
            exit 0
        ;;
        r)
            REPLACE=0
        ;;
        p)
            POPULATE=0
        ;;
        \?)
            error "Invalid option: -${OPTARG}"
            exit 1
        ;;
    esac
done

main

